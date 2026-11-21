CREATE TABLE storage.watchlist (
    watchlist_id uuid PRIMARY KEY,
    owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 64),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (owner_id, name)
);

CREATE TABLE storage.watchlist_member (
    watchlist_id uuid NOT NULL REFERENCES storage.watchlist(watchlist_id) ON DELETE CASCADE,
    ts_code text NOT NULL CHECK (ts_code ~ '^[0-9]{6}\.(SH|SZ)$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (watchlist_id, ts_code)
);

CREATE TABLE storage.alert_rule (
    rule_id uuid PRIMARY KEY,
    watchlist_id uuid NOT NULL REFERENCES storage.watchlist(watchlist_id) ON DELETE CASCADE,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    kind text NOT NULL CHECK (kind IN ('price_change','volume_ratio','pe','pb','financial_update','dividend')),
    comparator text NOT NULL CHECK (comparator IN ('above','below','abs_above')),
    threshold numeric,
    lookback_days integer NOT NULL CHECK (lookback_days BETWEEN 1 AND 60),
    cooldown_hours integer NOT NULL CHECK (cooldown_hours BETWEEN 0 AND 720),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (watchlist_id, name),
    CHECK ((kind IN ('financial_update','dividend')) = (threshold IS NULL))
);

CREATE TABLE storage.alert_event (
    event_id uuid PRIMARY KEY,
    rule_id uuid NOT NULL REFERENCES storage.alert_rule(rule_id) ON DELETE CASCADE,
    ts_code text NOT NULL CHECK (ts_code ~ '^[0-9]{6}\.(SH|SZ)$'),
    event_type text NOT NULL CHECK (event_type IN ('triggered','recovered')),
    status text NOT NULL CHECK (status IN ('active','recovered','acknowledged')),
    observation_date date NOT NULL,
    triggered_value numeric,
    threshold numeric,
    baseline jsonb NOT NULL DEFAULT '{}',
    source_kind text NOT NULL CHECK (source_kind IN ('market','financial','dividend')),
    source_versions text[] NOT NULL CHECK (cardinality(source_versions) > 0),
    dedup_key text NOT NULL UNIQUE CHECK (dedup_key ~ '^[0-9a-f]{64}$'),
    occurred_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz,
    acknowledged_by text,
    CHECK ((acknowledged_at IS NULL) = (acknowledged_by IS NULL))
);
CREATE INDEX alert_event_rule_time ON storage.alert_event(rule_id, occurred_at DESC);
CREATE UNIQUE INDEX alert_one_active_condition ON storage.alert_event(rule_id, ts_code)
    WHERE event_type='triggered' AND status IN ('active','acknowledged');

CREATE TABLE storage.in_app_notification (
    notification_id uuid PRIMARY KEY,
    event_id uuid NOT NULL REFERENCES storage.alert_event(event_id) ON DELETE CASCADE,
    owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 64),
    read_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, owner_id)
);
CREATE INDEX notification_owner_unread ON storage.in_app_notification(owner_id, created_at DESC)
    WHERE read_at IS NULL;
