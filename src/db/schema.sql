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

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    job_id TEXT,
    reason TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    type TEXT NOT NULL DEFAULT 'Normal'  -- Normal or Warning
);

CREATE TABLE IF NOT EXISTS hetzner_pricing (
    id TEXT PRIMARY KEY,
    server_type TEXT NOT NULL,
    price_hourly_eur REAL NOT NULL,
    price_monthly_eur REAL,
    volume_price_per_gb_month_eur REAL NOT NULL,
    last_updated INTEGER NOT NULL
);

-- Indices for common queries
CREATE INDEX IF NOT EXISTS idx_secrets_namespace ON secrets(namespace);
CREATE INDEX IF NOT EXISTS idx_cronjobs_namespace ON cronjobs(namespace);
CREATE INDEX IF NOT EXISTS idx_jobs_namespace ON jobs(namespace);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_cronjob ON jobs(cronjob_name);
CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
