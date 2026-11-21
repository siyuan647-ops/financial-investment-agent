# Dynamic specialist workflow

The production baseline allows 16,000 cumulative input-plus-output tokens per Fundamental,
News, Investment, or Risk role and 50,000 cumulative specialist tokens per deep task. Market has
no role-level cumulative-token gate:

```text
APP_DEEP_RESEARCH_ROLE_MAX_TOKENS=16000
APP_DEEP_RESEARCH_MAX_TOKENS=50000
```

The target-only path has no route or verify call. Per-call completion caps remain controlled by
the Qwen `generate` profile; raising task budgets does not raise that output cap.

The unified deep-research path always uses this specialist graph.

```text
Program security resolution
              |
                       +--------------+--------------+
                       |              |              |
                Fundamental Agent  News Agent   Market Agent
                       +--------------+--------------+
                                      |
                         Investment Analysis Agent
                              no tool permission
                                      |
                              Risk Review Agent
                              no tool permission
                                      |
                    Program packaging -> Partial report
```

Fundamental, News, and Market run concurrently with separate model/tool histories. The program overwrites
every model-proposed security code, `as_of`, and request ID. Fundamental is exposed only to four
fixed-window domain tools: `get_company_profile`, `get_profitability`, `get_balance_sheet`, and
`get_cash_flow`. It must obtain all four results before its Markdown can be accepted. Calls from one
turn execute concurrently, their compact results are returned on the next turn, and a normalized
tool-plus-arguments fingerprint rejects exact repeats. The model cannot widen dates or limits:
company profile includes the latest three key-metric periods, profitability includes two annual
periods plus four independent quarters, and balance sheet and cash flow each include four periods.
The auxiliary domain is not exposed. The loop has a 20 tool-round safety ceiling.

The 16,000-token Fundamental role budget reserves 10,000 tokens for final Markdown. A further tool
turn uses a 5,000-token preflight estimate. An already returned tool-call turn is still validated
and executed even if observed usage crosses the planning allowance, and already returned Markdown
is never discarded solely because of post-call usage. Fundamental has no role-level 120-second
timeout; the enclosing deep task retains its 300-second safety limit and each provider request
retains transport/total request bounds. The model output contract is only a Markdown string: the
program does not request JSON, claims, evidence-ID closure, code-fence removal, section filling, or
other Markdown normalization. Structured tool evidence, coverage, limitations, and redacted audit
records remain internal metadata for downstream processing.
News retains its existing dynamic single-tool loop and cannot finish before company news,
announcements, and earnings events have been attempted. At a tool, time, or model failure boundary,
existing results are retained in a deterministic degraded Markdown report instead of being
discarded.

Fundamental emits a Markdown report covering company profile, profitability, balance sheet, cash
flow, and data quality. News emits a separate Markdown report covering company
news, announcement availability, events, industry/macro effects, signals, and source coverage.
News has no market tool permission, so price action cannot be presented as news analysis.

The Investment Analysis Agent receives the three professional reports and cannot access tools or
raw tool results. Its prompt—not deterministic program logic—weighs the bullish and bearish cases,
records limitations, writes a detailed rationale and concrete non-personalized actions relative to
a standard allocation, and chooses exactly one of Buy, Overweight, Hold, Underweight, or Sell.
Conflict alone is not a reason to Hold. The primary model call uses a four-field internal Pydantic
schema (`recommendation`, `rationale`, `limitations`, and `strategic_actions`), then a deterministic
renderer immediately converts the parsed result to fixed-section Markdown. Only that Markdown leaves the agent, is
persisted in collaboration state, and reaches Risk Review. If structured output is unavailable or
invalid, the agent retries once as direct Markdown; a second failure produces a conservative
neutral Markdown report. Evidence IDs are not required at this stage.

The Risk Review Agent receives the Investment Markdown plus a trimmed view of each upstream report:
only `role`, `summary`, and the complete `report_markdown`; coverage, claims, and limitations metadata
are not repeated outside the report body. It cannot access tools. It reviews business, financial,
data-quality, confidence, conflict, liquidity, concentration, valuation-trap, sizing, entry, and
stop-loss risks. It maps Buy/Overweight/Hold/Underweight/Sell to the existing Chinese five-level
scale and may uphold, adjust, veto, or force a neutral result. Risk now returns one complete Markdown
document directly, with fixed sections for the final rating, risk assessment, final rationale,
trading actions, and rating adjustment. It does not use a JSON Schema or Pydantic response model,
and the program does not parse a rating or override flag from its prose. The runtime gate checks only
that the response is non-empty and has meaningful length; output semantics and the five-level choice
remain prompt requirements. A failed or too-short response produces deterministic conservative
Markdown that explicitly says the review is incomplete and does not assign a final rating. The
previous independent Final Qwen call is removed; the program appends Investment and Risk Markdown
directly into `ResearchReport` without adding analysis. No citation
closure, semantic Verify, or correction call follows. The
report is persisted as `partial` with `evidence_validation_deferred`,
`semantic_verification_skipped`, and `content_gate_skipped`.

The persisted execution contains five bounded audit records: role input context, selected tools,
structured/Markdown output, prompt version, status, and token count for Fundamental, News, Market,
Investment, and Risk. Investment audit output stores the rendered Markdown rather than its internal
structured draft; Risk audit output stores its complete Markdown as `report_markdown`. Qwen
invocation metadata remains separately linked by task ID. Market runs
in parallel with Fundamental and News. Its harness exposes only `get_stock_data`, then only
`get_indicators`, and finally no tools, so the two data calls are programmatically serialized.
The model-facing `get_stock_data` schema accepts an empty object only; the harness owns the code,
analysis time, cutoff date, and 60-bar window. The model-facing `get_indicators` schema accepts
only `indicators`; the harness discards every other model-supplied field and injects the code,
analysis time, cutoff date, and 30-day output window. The retained indicator list is still checked
against the allowlist, the one-to-eight bound, and uniqueness before execution.
Market's model-facing tool context preserves all 60 ordered OHLCV bars and all 30 complete
indicator dates, while omitting item evidence IDs and the evidence list. Tool evidence is still
retained internally for audit and lineage. Its final model response is direct Markdown rather
than structured claims or JSON. Market has no role-level cumulative-token gate or role-level
timeout; the enclosing deep-task budget and provider request/output safety limits remain in effect.

Fundamental and News audit records also persist bounded, redacted `tool_attempts`. Each attempt
contains its sequence, Qwen invocation ID, allowlisted tool name, allowlisted argument keys,
sanitized code/date/enum/limit values, terminal processing stage, outcome, and a safe error code.
Unknown argument names are represented only by a count; free-form query text, exception messages,
prompts, model responses, credentials, and request tokens are never stored. Failed attempts are
recorded even when they fail before a tool result exists. Diagnostic error codes distinguish
invalid tool names, invalid dates, schema-invalid arguments, execution failures, response contract
failures, and duplicate attempts. These records diagnose new tasks only; they cannot reconstruct
model responses from tasks completed before this audit was introduced.

The DataOps adapter reads Fundamental's four fixed windows directly from the five-domain business
tables and returns evidence-backed compact records. Missing required record types or periods remain
explicit gaps; the auxiliary domain is retained in storage but has no Fundamental tool. The
orchestrator's legacy adapters still map company, finance, market, events, and deterministic
calculations to the existing evidence-backed standard tools. Unsupported event categories remain
explicit gaps.
For `market.query`, requested ranges longer than 46 calendar days are treated as a latest-year
research request ending at the injected task `as_of`. The adapter divides that year into
continuous, non-overlapping windows whose inclusive length is at most 46 days, queries bars and
valuations per window, and retains successful windows if another window fails. `quote` on a long
request is resolved through the batched bars path so the latest available trading day can be used
even when `as_of` is not itself a published market date. Full rows remain in the data service;
the model context keeps only the latest record from each batch plus coverage metadata. Requests of
46 days or less retain their original dates and unsampled behavior for fast analysis.
The News Agent is exposed only to `get_stock_news`, `get_announcements`, and
`get_earnings_events` when the provider is configured, and must call all three.
Each tool is fixed to the seven calendar days ending at the injected `as_of`, returns at most ten
combined records, and cannot be widened by the model. The adapters use `news` plus `major_news`,
`anns_d`, and `forecast` plus `express`, respectively. Permission or source failures remain
explicit and incomplete coverage is never represented as “no adverse news”.
