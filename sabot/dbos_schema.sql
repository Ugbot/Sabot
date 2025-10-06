-- DBOS Control Plane Database Schema (SQLite compatible)
-- Supports durable job orchestration, task assignment, and agent coordination
-- Works with both SQLite (local mode) and PostgreSQL (distributed mode)

-- ============================================================================
-- AGENTS TABLE: Worker node registration and health
-- ============================================================================
CREATE TABLE agents (
    agent_id TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
        -- 'unknown', 'alive', 'dead', 'joining', 'leaving'

    -- Capabilities
    max_workers INTEGER DEFAULT 8,
    available_slots INTEGER DEFAULT 8,
    cython_available BOOLEAN DEFAULT 0,
    gpu_available BOOLEAN DEFAULT 0,

    -- Resource tracking
    cpu_percent REAL DEFAULT 0.0,
    memory_percent REAL DEFAULT 0.0,
    disk_percent REAL DEFAULT 0.0,

    -- Health monitoring
    last_heartbeat TEXT DEFAULT CURRENT_TIMESTAMP,
    registered_at TEXT DEFAULT CURRENT_TIMESTAMP,

    -- Metrics
    total_tasks_executed INTEGER DEFAULT 0,
    total_rows_processed INTEGER DEFAULT 0,
    total_bytes_processed INTEGER DEFAULT 0,

    CHECK (port > 0 AND port < 65536)
);

CREATE INDEX idx_agents_status ON agents(status);
CREATE INDEX idx_agents_heartbeat ON agents(last_heartbeat);

-- ============================================================================
-- JOBS TABLE: Job definitions and state
-- ============================================================================
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,

    -- Job configuration
    default_parallelism INTEGER DEFAULT 4,
    checkpoint_interval_ms INTEGER DEFAULT 60000,
    state_backend TEXT DEFAULT 'tonbo',
        -- 'tonbo', 'rocksdb', 'memory', 'redis'

    -- Job DAG (stored as JSON)
    job_graph_json TEXT NOT NULL,
    optimized_dag_json TEXT,  -- Optimized version after plan optimization

    -- Execution state
    status TEXT NOT NULL DEFAULT 'pending',
        -- 'pending', 'optimizing', 'compiling', 'deploying', 'running',
        -- 'completed', 'failed', 'canceling', 'canceled'

    -- Timestamps
    submitted_at TEXT DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    completed_at TEXT,

    -- Error tracking
    error_message TEXT,
    failure_count INTEGER DEFAULT 0,

    -- Metadata
    user_id TEXT,
    priority INTEGER DEFAULT 1,

    CHECK (priority >= 0)
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_submitted ON jobs(submitted_at);
CREATE INDEX idx_jobs_user ON jobs(user_id);

-- ============================================================================
-- EXECUTION_GRAPHS TABLE: Physical execution plans
-- ============================================================================
CREATE TABLE execution_graphs (
    job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,

    -- Physical plan (stored as JSON)
    graph_json TEXT NOT NULL,

    -- Metadata
    total_tasks INTEGER NOT NULL,
    total_operators INTEGER NOT NULL,

    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TASKS TABLE: Individual task instances (TaskVertex)
-- ============================================================================
CREATE TABLE tasks (
    task_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,

    -- Task identity
    operator_id TEXT NOT NULL,
    operator_type TEXT NOT NULL,
    operator_name TEXT,
    task_index INTEGER NOT NULL,
    parallelism INTEGER NOT NULL,

    -- Deployment
    agent_id TEXT REFERENCES agents(agent_id) ON DELETE SET NULL,
    slot_id TEXT,

    -- Task state
    state TEXT NOT NULL DEFAULT 'created',
        -- 'created', 'scheduled', 'deploying', 'running', 'finished',
        -- 'canceling', 'canceled', 'failed', 'reconciling'
    state_updated_at TEXT DEFAULT CURRENT_TIMESTAMP,

    -- Configuration (stored as JSON)
    parameters_json TEXT,
    stateful BOOLEAN DEFAULT 0,
    key_by_columns TEXT,  -- JSON array of column names for partitioning

    -- Resources
    memory_mb INTEGER DEFAULT 256,
    cpu_cores REAL DEFAULT 0.5,

    -- Metrics
    rows_processed INTEGER DEFAULT 0,
    bytes_processed INTEGER DEFAULT 0,
    failure_count INTEGER DEFAULT 0,
    last_heartbeat TEXT,

    -- Error tracking
    error_message TEXT,

    CHECK (task_index >= 0),
    CHECK (parallelism > 0)
);

CREATE INDEX idx_tasks_job ON tasks(job_id);
CREATE INDEX idx_tasks_agent ON tasks(agent_id);
CREATE INDEX idx_tasks_state ON tasks(state);
CREATE INDEX idx_tasks_operator ON tasks(operator_id);

-- ============================================================================
-- TASK_ASSIGNMENTS TABLE: Track task assignment history (audit log)
-- ============================================================================
CREATE TABLE task_assignments (
    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
    operator_type TEXT NOT NULL,

    -- Assignment metadata
    assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,  -- 'initial_deploy', 'rescale', 'failure_recovery'

    UNIQUE(task_id, assigned_at)
);

CREATE INDEX idx_assignments_job ON task_assignments(job_id);
CREATE INDEX idx_assignments_task ON task_assignments(task_id);
CREATE INDEX idx_assignments_agent ON task_assignments(agent_id);

-- ============================================================================
-- SHUFFLE_EDGES TABLE: Data flow between operators
-- ============================================================================
CREATE TABLE shuffle_edges (
    shuffle_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,

    -- Operators connected by this shuffle
    upstream_operator_id TEXT NOT NULL,
    downstream_operator_id TEXT NOT NULL,

    -- Shuffle configuration
    edge_type TEXT NOT NULL,
        -- 'forward', 'hash', 'broadcast', 'rebalance', 'range', 'custom'
    partition_keys TEXT,
    num_partitions INTEGER DEFAULT 1,

    -- Task mappings (stored as JSON arrays)
    upstream_task_ids TEXT NOT NULL,  -- JSON array
    downstream_task_ids TEXT NOT NULL,  -- JSON array

    -- Metrics
    rows_shuffled INTEGER DEFAULT 0,
    bytes_shuffled INTEGER DEFAULT 0,

    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_shuffle_job ON shuffle_edges(job_id);
CREATE INDEX idx_shuffle_upstream ON shuffle_edges(upstream_operator_id);
CREATE INDEX idx_shuffle_downstream ON shuffle_edges(downstream_operator_id);

-- ============================================================================
-- WORKFLOW_STATE TABLE: DBOS workflow execution state
-- ============================================================================
CREATE TABLE workflow_state (
    workflow_id TEXT PRIMARY KEY,
    workflow_type TEXT NOT NULL,
        -- 'job_submission', 'rescale_operator', 'failure_recovery'

    -- Associated resources
    job_id TEXT REFERENCES jobs(job_id) ON DELETE CASCADE,
    operator_id TEXT,

    -- Workflow execution
    current_step TEXT NOT NULL,
    completed_steps TEXT,  -- JSON array of completed step names
    status TEXT NOT NULL DEFAULT 'running',
        -- 'running', 'completed', 'failed', 'paused'

    -- Workflow data (stored as JSON)
    context_json TEXT,

    -- Timestamps
    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,

    -- Error tracking
    error_message TEXT
);

CREATE INDEX idx_workflow_job ON workflow_state(job_id);
CREATE INDEX idx_workflow_type ON workflow_state(workflow_type);
CREATE INDEX idx_workflow_status ON workflow_state(status);

-- ============================================================================
-- AGENT_HEARTBEATS TABLE: Rolling window of agent health (time-series)
-- ============================================================================
CREATE TABLE agent_heartbeats (
    heartbeat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,

    -- Resource snapshot at heartbeat time
    cpu_percent REAL,
    memory_percent REAL,
    disk_percent REAL,
    active_workers INTEGER,
    total_processed INTEGER,

    -- Timestamp
    recorded_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_heartbeats_agent ON agent_heartbeats(agent_id);
CREATE INDEX idx_heartbeats_time ON agent_heartbeats(recorded_at);

-- ============================================================================
-- Helper views for common queries
-- ============================================================================

-- Active jobs with task counts
CREATE VIEW active_jobs_summary AS
SELECT
    j.job_id,
    j.job_name,
    j.status,
    COUNT(t.task_id) as total_tasks,
    COUNT(CASE WHEN t.state = 'running' THEN 1 END) as running_tasks,
    COUNT(CASE WHEN t.state = 'failed' THEN 1 END) as failed_tasks,
    SUM(t.rows_processed) as total_rows,
    SUM(t.bytes_processed) as total_bytes,
    j.submitted_at,
    j.started_at
FROM jobs j
LEFT JOIN tasks t ON j.job_id = t.job_id
WHERE j.status IN ('running', 'deploying', 'pending')
GROUP BY j.job_id, j.job_name, j.status, j.submitted_at, j.started_at;

-- Healthy agents with available capacity
CREATE VIEW available_agents AS
SELECT
    agent_id,
    host,
    port,
    available_slots,
    max_workers,
    cpu_percent,
    memory_percent,
    last_heartbeat,
    (strftime('%s', 'now') - strftime('%s', last_heartbeat)) as seconds_since_heartbeat
FROM agents
WHERE status = 'alive'
  AND (strftime('%s', 'now') - strftime('%s', last_heartbeat)) < 30  -- Heartbeat within 30 seconds
  AND available_slots > 0
ORDER BY available_slots DESC;

-- Rescaling operations (tracks live rescaling of operators)
CREATE TABLE rescaling_operations (
    rescaling_id TEXT PRIMARY KEY,                    -- Unique rescaling operation ID
    job_id TEXT NOT NULL,                             -- Job being rescaled
    operator_id TEXT NOT NULL,                         -- Operator being rescaled
    old_parallelism INTEGER NOT NULL,                  -- Original parallelism
    new_parallelism INTEGER NOT NULL,                  -- Target parallelism
    status TEXT NOT NULL DEFAULT 'pending',           -- pending, in_progress, completed, failed
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- When rescaling started
    completed_at TIMESTAMP,                            -- When rescaling finished
    error_message TEXT,                                -- Error details if failed

    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,

    -- Ensure only one active rescaling per operator
    UNIQUE(job_id, operator_id, status) ON CONFLICT REPLACE
);

-- Indexes for rescaling operations
CREATE INDEX idx_rescaling_job ON rescaling_operations(job_id);
CREATE INDEX idx_rescaling_status ON rescaling_operations(status);

-- Task assignment load per agent
CREATE VIEW agent_task_load AS
SELECT
    a.agent_id,
    a.host,
    a.status,
    COUNT(t.task_id) as assigned_tasks,
    a.max_workers,
    a.available_slots,
    ROUND((CAST(COUNT(t.task_id) AS REAL) / a.max_workers) * 100, 2) as utilization_percent
FROM agents a
LEFT JOIN tasks t ON a.agent_id = t.agent_id AND t.state IN ('running', 'deploying', 'scheduled')
GROUP BY a.agent_id, a.host, a.status, a.max_workers, a.available_slots
ORDER BY utilization_percent ASC;
