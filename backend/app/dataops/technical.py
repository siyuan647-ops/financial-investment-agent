"""Bounded OHLCV ingestion and deterministic technical indicators."""

import asyncio
import hashlib
from datetime import date, datetime
from decimal import Decimal, localcontext
from uuid import uuid4

from psycopg import sql

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.contracts.mapping import content_version, upstream_date
from app.contracts.technical import TechnicalIndicatorRecord
from app.dataops.market import FIELDS, MarketError, decimal_rows, normalize
from app.dataops.reference import PROVIDER, security_key
from app.providers.tushare import ProviderError
from app.repositories.storage import identity

INDICATOR_PARAMETERS = {
    "close_50_sma": {"period": 50},
    "close_10_ema": {"period": 10},
    "macd": {"fast": 12, "slow": 26},
    "macds": {"fast": 12, "slow": 26, "signal": 9},
    "macdh": {"fast": 12, "slow": 26, "signal": 9},
    "rsi": {"period": 14},
    "boll": {"period": 20, "standard_deviations": Decimal("2")},
    "boll_ub": {"period": 20, "standard_deviations": Decimal("2")},
    "boll_lb": {"period": 20, "standard_deviations": Decimal("2")},
    "atr": {"period": 14},
    "vwma": {"period": 20},
}


async def collect_ohlcv_window(
    adapter,
    repository,
    raw_store,
    code: str,
    trading_dates: tuple[date, ...],
    *,
    timeout_seconds=180,
):
    security_key(code)
    if len(trading_dates) < 90 or tuple(sorted(set(trading_dates))) != trading_dates:
        raise MarketError("at_least_90_unique_trading_dates_required")
    start, end = trading_dates[0], trading_dates[-1]
    run_id = await repository.create_run(
        "technical-ohlcv-v1:" + uuid4().hex,
        {"ts_code": code, "start_date": start.isoformat(), "end_date": end.isoformat()},
    )
    await repository.transition_run(run_id, "queued", "running")
    try:
        async with asyncio.timeout(timeout_seconds):
            params = {
                "ts_code": code,
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
            }
            response = await adapter.query("daily", params, FIELDS["daily"])
            fetched = datetime.now(SHANGHAI)
            raw_id = await raw_store.save(
                run_id=run_id,
                provider=PROVIDER,
                source_api="daily",
                parameters={**params, "fields": FIELDS["daily"]},
                fetched_at=fetched,
                content=response.raw_content,
            )
            rows = decimal_rows(response.raw_content)
            actual = {upstream_date(row["trade_date"]) for row in rows}
            if (
                len(rows) != len(actual)
                or len(actual) < 90
                or any(day < start or day > end for day in actual)
            ):
                raise MarketError("ohlcv_window_not_complete")
            rows.sort(key=lambda row: upstream_date(row["trade_date"]))
            rows = rows[-90:]
            staged = []
            for row in rows:
                provenance = Provenance(
                    provider=PROVIDER,
                    source_api="daily",
                    source_ref=str(raw_id),
                    raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
                    request_id=run_id.hex,
                    data_version="0" * 64,
                    mapping_version="step08-v1",
                    timing=Availability(fetched_at=fetched, available_at=fetched, basis="observed"),
                    quality="valid",
                    freshness="fresh",
                )
                staged.append(normalize("daily", row, provenance))
            inserted = 0
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600637)")
                for record in staged:
                    kind, key, semantic = identity(record)
                    cursor = await repository.connection.execute(
                        "SELECT semantic_hash,data_version FROM storage.record_version "
                        "WHERE kind=%s AND business_key=%s ORDER BY available_at DESC,"
                        "fetched_at DESC,data_version DESC LIMIT 1",
                        (kind, key),
                    )
                    previous = await cursor.fetchone()
                    previous_present = False
                    if previous:
                        cursor = await repository.connection.execute(
                            sql.SQL(
                                "SELECT EXISTS(SELECT 1 FROM storage.{} WHERE data_version=%s)"
                            ).format(sql.Identifier(kind)),
                            (previous[1],),
                        )
                        previous_present = (await cursor.fetchone())[0]
                    version = (
                        previous[1]
                        if previous and previous[0] == semantic and previous_present
                        else content_version(
                            {"semantic": semantic, "observed_at": fetched.isoformat()}
                        )
                    )
                    record = record.model_copy(
                        update={
                            "provenance": record.provenance.model_copy(
                                update={"data_version": version}
                            )
                        }
                    )
                    inserted += await repository.put(record, raw_id)
                result = {
                    "run_id": str(run_id),
                    "ts_code": code,
                    "records": len(staged),
                    "inserted": inserted,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else str(exc)
            if isinstance(exc, MarketError)
            else type(exc).__name__
        )
        await repository.transition_run(
            run_id,
            "running",
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
            result={"reason": reason},
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise MarketError(f"technical_ohlcv_pipeline_failed:{reason}") from None


def calculate_technical_indicators(bars, *, output_days=30):
    bars = tuple(sorted(bars, key=lambda item: item.trade_date))
    if len(bars) < 90 or not 1 <= output_days <= 30:
        raise ValueError("technical_indicators_require_90_bars")
    closes = [bar.close.value for bar in bars]
    highs = [bar.high.value for bar in bars]
    lows = [bar.low.value for bar in bars]
    volumes = [bar.volume.value for bar in bars]
    if any(value is None for value in (*closes, *highs, *lows, *volumes)):
        raise ValueError("technical_indicator_input_missing")

    sma50 = _sma(closes, 50)
    ema10 = _ema(closes, 10)
    ema12, ema26 = _ema(closes, 12), _ema(closes, 26)
    macd = [None if a is None or b is None else a - b for a, b in zip(ema12, ema26, strict=True)]
    macds = _ema_sparse(macd, 9)
    macdh = [None if a is None or b is None else a - b for a, b in zip(macd, macds, strict=True)]
    rsi = _rsi(closes, 14)
    boll, boll_ub, boll_lb = _boll(closes, 20, Decimal("2"))
    atr = _atr(highs, lows, closes, 14)
    vwma = _vwma(closes, volumes, 20)
    series = {
        "close_50_sma": sma50,
        "close_10_ema": ema10,
        "macd": macd,
        "macds": macds,
        "macdh": macdh,
        "rsi": rsi,
        "boll": boll,
        "boll_ub": boll_ub,
        "boll_lb": boll_lb,
        "atr": atr,
        "vwma": vwma,
    }
    records = []
    start = len(bars) - output_days
    for index in range(start, len(bars)):
        for name, values in series.items():
            value = values[index]
            if value is None:
                raise ValueError(f"indicator_warmup_incomplete:{name}")
            relevant = bars[: index + 1]
            latest = relevant[-1]
            records.append(
                TechnicalIndicatorRecord(
                    security=latest.security,
                    trade_date=latest.trade_date,
                    indicator=name,
                    value=value,
                    parameters=INDICATOR_PARAMETERS[name],
                    input_versions=tuple(item.provenance.data_version for item in relevant),
                    provenance=latest.provenance.model_copy(
                        update={
                            "source_api": "technical_indicator",
                            "source_ref": f"{latest.security.ts_code}:{latest.trade_date}:{name}",
                            "data_version": "0" * 64,
                            "mapping_version": "technical-indicator-v1",
                        }
                    ),
                )
            )
    return tuple(records)


def _sma(values, period):
    result = [None] * len(values)
    for index in range(period - 1, len(values)):
        result[index] = sum(values[index - period + 1 : index + 1], Decimal(0)) / Decimal(period)
    return result


def _ema(values, period):
    result = [None] * len(values)
    if len(values) < period:
        return result
    result[period - 1] = sum(values[:period], Decimal(0)) / Decimal(period)
    alpha = Decimal(2) / Decimal(period + 1)
    for index in range(period, len(values)):
        result[index] = (values[index] - result[index - 1]) * alpha + result[index - 1]
    return result


def _ema_sparse(values, period):
    result = [None] * len(values)
    indexes = [index for index, value in enumerate(values) if value is not None]
    if len(indexes) < period:
        return result
    seed = indexes[period - 1]
    result[seed] = sum((values[index] for index in indexes[:period]), Decimal(0)) / Decimal(period)
    alpha = Decimal(2) / Decimal(period + 1)
    for index in indexes[period:]:
        previous = result[index - 1]
        if previous is None:
            previous = next(
                result[item] for item in range(index - 1, -1, -1) if result[item] is not None
            )
        result[index] = (values[index] - previous) * alpha + previous
    return result


def _rsi(values, period):
    result = [None] * len(values)
    changes = [values[index] - values[index - 1] for index in range(1, len(values))]
    gains = [max(value, Decimal(0)) for value in changes]
    losses = [max(-value, Decimal(0)) for value in changes]
    average_gain = sum(gains[:period], Decimal(0)) / Decimal(period)
    average_loss = sum(losses[:period], Decimal(0)) / Decimal(period)
    result[period] = _rsi_value(average_gain, average_loss)
    for index in range(period + 1, len(values)):
        average_gain = (average_gain * Decimal(period - 1) + gains[index - 1]) / Decimal(period)
        average_loss = (average_loss * Decimal(period - 1) + losses[index - 1]) / Decimal(period)
        result[index] = _rsi_value(average_gain, average_loss)
    return result


def _rsi_value(gain, loss):
    if loss == 0:
        return Decimal(100) if gain > 0 else Decimal(50)
    return Decimal(100) - Decimal(100) / (Decimal(1) + gain / loss)


def _boll(values, period, deviations):
    middle = _sma(values, period)
    upper, lower = [None] * len(values), [None] * len(values)
    for index in range(period - 1, len(values)):
        window = values[index - period + 1 : index + 1]
        mean = middle[index]
        with localcontext() as context:
            context.prec = 50
            variance = sum(((item - mean) ** 2 for item in window), Decimal(0)) / Decimal(period)
            deviation = variance.sqrt()
        upper[index], lower[index] = mean + deviations * deviation, mean - deviations * deviation
    return middle, upper, lower


def _atr(highs, lows, closes, period):
    result = [None] * len(closes)
    true_ranges = [
        max(
            highs[index] - lows[index],
            abs(highs[index] - closes[index - 1]),
            abs(lows[index] - closes[index - 1]),
        )
        for index in range(1, len(closes))
    ]
    average = sum(true_ranges[:period], Decimal(0)) / Decimal(period)
    result[period] = average
    for index in range(period + 1, len(closes)):
        average = (average * Decimal(period - 1) + true_ranges[index - 1]) / Decimal(period)
        result[index] = average
    return result


def _vwma(closes, volumes, period):
    result = [None] * len(closes)
    for index in range(period - 1, len(closes)):
        price_window = closes[index - period + 1 : index + 1]
        volume_window = volumes[index - period + 1 : index + 1]
        denominator = sum(volume_window, Decimal(0))
        if denominator == 0:
            raise ValueError("vwma_zero_volume_window")
        numerator = sum(
            (price * volume for price, volume in zip(price_window, volume_window, strict=True)),
            Decimal(0),
        )
        result[index] = numerator / denominator
    return result
