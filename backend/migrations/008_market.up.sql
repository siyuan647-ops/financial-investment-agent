-- A watermark certifies only this exact security/window, never uncollected history.
CREATE TABLE storage.market_watermark (
    provider text NOT NULL,
    ts_code text NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    published_through date NOT NULL,
    run_id uuid NOT NULL REFERENCES storage.pipeline_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, ts_code, window_start, window_end),
    CHECK (window_start <= window_end AND published_through = window_end)
);
