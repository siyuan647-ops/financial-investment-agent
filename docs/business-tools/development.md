# Business tools v1

This layer registers bounded internal tools for specialist research agents. It is
not an MCP server and it does not expose Tushare, SQL, Python, or free-form expressions to a
model. Production handlers must read the existing business database, standard data API,
approved document index, or Step27 text-monitoring store.

## Registered tools

| Tool | Main domains | Runtime capability |
| --- | --- | --- |
| `security.resolve` | security master and aliases | available |
| `company.snapshot` | security, listing, industry, coverage | available |
| `finance.query` | normalized financial facts and ratios | available |
| `market.query` | quote, daily bars, valuation | available |
| `events.query` | corporate actions | partial |
| `disclosures.search` | approved announcements and reports | coverage-dependent |
| `news.search` | confirmed `major_news` sources | coverage-dependent |
| `get_stock_news` | `news` and `major_news` company-matched items | coverage-dependent |
| `get_announcements` | `anns_d` company announcements | coverage-dependent |
| `get_earnings_events` | `forecast` and `express` events | coverage-dependent |
| `get_company_profile` | company identity, industry, business segments, holders, latest three key-metric periods | available |
| `get_profitability` | latest two annual periods plus four independent quarters | available |
| `get_balance_sheet` | latest four quarter-end point-in-time balance sheets | available |
| `get_cash_flow` | latest four independent-quarter cash-flow statements | available |
| `calculate.run` | four deterministic derived metrics | available |

`events.query` must return an explicit gap when forecast, repurchase, pledge, holder change,
or suspension data is not collected. `disclosures.search` and `news.search` must return
`coverage_incomplete` or `source_unavailable` when sources are disabled, unsynchronised, or
partial; an empty result must never be interpreted as “no adverse news”.

The News Agent uses three program-bounded tools. Every tool derives an inclusive seven-calendar-
day window from the injected task `as_of`, rejects future data, sorts newest first, deduplicates,
and returns at most ten combined records. `get_stock_news` combines selected `news` and
`major_news` sources and matches the resolved company name or code locally; because neither feed
offers a complete company-news universe, it always reports incomplete coverage.
`get_announcements` queries `anns_d` by exact `ts_code` and each date in the window.
`get_earnings_events` combines exact-code `forecast` and `express` announcements. Permission,
source, and partial-day failures remain explicit gaps; an unavailable source never becomes a
clean “no adverse event” result. The legacy `news.search` contract remains for orchestrator
compatibility but is no longer exposed to the News Agent.

## Agent permissions

- Orchestrator: all registered tools; it alone resolves user-entered security names/codes.
- Fundamental: only `get_company_profile`, `get_profitability`, `get_balance_sheet`, and
  `get_cash_flow`. The auxiliary domain remains stored but is intentionally not exposed as a
  Fundamental tool.
- News: only `get_stock_news`, `get_announcements`, and `get_earnings_events` when the provider
  is configured.
- Risk: no data tools. It consumes the frozen Fundamental and News outputs and their evidence.
- Strategy: no data tools; the revised workflow no longer invokes this legacy role.

The registry validates a discriminated request contract before invoking a handler, enforces the
role allow-list and per-tool record ceiling, and verifies that handler responses retain the same
request and tool identity. Missing handler configuration is an explicit `source_unavailable`
result rather than fabricated data.

## Integration boundary

Create one handler per registered name and inject it into `BusinessToolRegistry`. Handlers map
only to the current repositories and standard evidence tools (`quote`, `bars`, `valuation`,
`financials`, `dividends`, `industry`, `calculation`). They return compact facts/citations plus
coverage metadata; full records remain server-side. The deep-research worker exposes the four
fixed-window domain readers to Fundamental and only the three bounded recent-information handlers
to News for every target-only research task. The model cannot supply date ranges or limits to the
four Fundamental readers; code, `as_of`, and request ID remain program-controlled.
`disclosures.search` is omitted from model metadata until a real handler is available; News cannot
call `market.query`.
