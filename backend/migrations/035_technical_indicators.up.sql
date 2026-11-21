CREATE TABLE storage.technical_indicator (
    business_key text NOT NULL,
    data_version text NOT NULL REFERENCES storage.record_version,
    ts_code text NOT NULL CHECK (ts_code ~ '^[0-9]{6}\.(SH|SZ)$'),
    trade_date date NOT NULL,
    indicator_name text NOT NULL CHECK (
        indicator_name IN (
            'close_50_sma','close_10_ema','macd','macds','macdh','rsi',
            'boll','boll_ub','boll_lb','atr','vwma'
        )
    ),
    indicator_value numeric NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (trade_date, data_version)
);
CREATE INDEX technical_indicator_lookup
    ON storage.technical_indicator(ts_code, indicator_name, trade_date DESC);
CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON storage.technical_indicator
    FOR EACH ROW EXECUTE FUNCTION storage.reject_mutation();
