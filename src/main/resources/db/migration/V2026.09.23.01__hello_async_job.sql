CREATE TABLE hello_async_job (
    id        UUID PRIMARY KEY,
    created   TIMESTAMPTZ NOT NULL,
    modified  TIMESTAMPTZ NOT NULL,
    status    VARCHAR(20) NOT NULL,
    version   INTEGER NOT NULL DEFAULT 0,
    json_data JSONB NOT NULL
);

CREATE INDEX idx_hello_async_job_status ON hello_async_job (status);
