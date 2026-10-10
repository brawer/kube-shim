-- SQLite schema for Kubernetes API Shim
-- All timestamps are Unix epoch seconds

CREATE TABLE IF NOT EXISTS secrets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'default',
    data BLOB NOT NULL,  -- JSON blob
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS cronjobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'default',
    spec BLOB NOT NULL,  -- JSON blob
    status BLOB,         -- JSON blob
    schedule TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'default',
    cronjob_name TEXT,
    spec BLOB NOT NULL,  -- JSON blob
    status TEXT NOT NULL DEFAULT 'Created',  -- Created, VolumePending, VolumeCreating, etc.
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    volume_id TEXT,
    volume_device TEXT,
    mount_point TEXT,
    worker_vm_id TEXT,
    worker_vm_name TEXT,
    worker_ssh_ip TEXT,
    exit_code INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_transition_time INTEGER,
    version INTEGER NOT NULL DEFAULT 1
);

-- Generic ephemeral volumes (Phase 5): one row per job *run*, not a
-- durable row reused across runs -- nothing here is ever retained, so
-- there's no Pending/Bound/Released lifecycle to track. Not yet written
-- to by anything: reserved schema, populated once the reconciliation
-- loop (Phase 6+) actually creates job runs to attach volumes to.
CREATE TABLE IF NOT EXISTS job_volumes (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    size_gb INTEGER NOT NULL,
    storage_class_name TEXT NOT NULL,
    provider_volume_id TEXT,
    mount_point TEXT,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    job_id TEXT,
    reason TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    type TEXT NOT NULL DEFAULT 'Normal'  -- Normal or Warning
);

-- Real, measured worker-VM metrics (not estimated from resources.requests
-- the way api::metrics used to): one row per job, upserted roughly every
-- 30s by reconcile::metrics_collector from VMRunning through
-- ContainerRunning (SSH is reachable from VMRunning onward -- see that
-- module's own docs). Deliberately "latest sample" semantics, not a time
-- series -- matching how real Kubernetes metrics-server itself only ever
-- serves the most recent window, not history.
--
-- cpu_millicores/memory_usage_bytes are nullable: NULL means no
-- container is running yet (still provisioning), not zero usage --
-- node_* columns are still recorded in that case, independent of
-- whether a container exists (see reconcile::metrics_collector's own
-- docs on why Node-level and Pod-level readiness aren't the same gate).
CREATE TABLE IF NOT EXISTS worker_metrics (
    job_id TEXT PRIMARY KEY,
    cpu_millicores INTEGER,
    memory_usage_bytes INTEGER,
    node_memory_total_bytes INTEGER NOT NULL,
    node_memory_used_bytes INTEGER NOT NULL,
    node_cpu_count INTEGER NOT NULL,
    node_load1 REAL NOT NULL,
    sampled_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS hetzner_pricing (
    id TEXT PRIMARY KEY,
    server_type TEXT NOT NULL,
    price_hourly_eur REAL NOT NULL,
    price_monthly_eur REAL,
    volume_price_per_gb_month_eur REAL NOT NULL,
    last_updated INTEGER NOT NULL
);

-- Real UpCloud pricing (Phase 14a), synced daily from `GET /1.3/price`
-- -- one row per (zone, price_key) this project actually needs
-- (`workload::all_server_plans()` + both `volumes::StorageTier`s), not
-- the whole catalog. `currency` is the account's own real billing
-- currency, read from the response itself (`providers::PriceEntry`'s
-- own docs) -- never hardcoded, since UpCloud bills some accounts in
-- USD, not just EUR.
CREATE TABLE IF NOT EXISTS provider_pricing (
    zone TEXT NOT NULL,
    price_key TEXT NOT NULL,
    amount REAL NOT NULL,
    price REAL NOT NULL,
    currency TEXT NOT NULL,
    fetched_at INTEGER NOT NULL,
    PRIMARY KEY (zone, price_key)
);

-- The ECB's daily reference rates (Phase 14a), EUR-anchored (every row
-- is "1 EUR = `rate` `currency`") -- see src/currency.rs's own docs for
-- why converting between two non-EUR currencies pivots through EUR
-- rather than looking up a direct rate this feed never provides.
-- Replaced wholesale on every sync (src/currency.rs::store_rates), not
-- accumulated -- the ECB publishes one complete daily snapshot, not an
-- incremental diff.
CREATE TABLE IF NOT EXISTS exchange_rates (
    currency TEXT PRIMARY KEY,
    rate REAL NOT NULL,
    fetched_at INTEGER NOT NULL
);

-- Rolling budget guard (Phase 14b): a single row (`id` is always 1,
-- enforced by the CHECK) holding the current balance plus the three
-- settings `PATCH /settings` can change live. `config.toml`'s `[shim]`
-- fields only ever seed this row once, on first boot
-- (`pricing::ensure_budget_seeded`) -- every real read/write after
-- that goes through this table, never `config.toml` again (same
-- bootstrap-once convention `server.api_tokens` already uses).
CREATE TABLE IF NOT EXISTS budget_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    balance REAL NOT NULL,
    last_accrual_at INTEGER NOT NULL,
    main_currency TEXT NOT NULL,
    budget_daily_rate REAL NOT NULL,
    budget_rollover_cap_days INTEGER NOT NULL
);

-- Indices for common queries
CREATE INDEX IF NOT EXISTS idx_secrets_namespace ON secrets(namespace);
CREATE INDEX IF NOT EXISTS idx_cronjobs_namespace ON cronjobs(namespace);
CREATE INDEX IF NOT EXISTS idx_jobs_namespace ON jobs(namespace);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_cronjob ON jobs(cronjob_name);
CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_job_volumes_job ON job_volumes(job_id);
