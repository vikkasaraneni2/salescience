CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    organization_id VARCHAR(255),
    user_id VARCHAR(255),
    company VARCHAR(255) NOT NULL,
    result JSONB
);

CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) REFERENCES jobs(job_id),
    document_type VARCHAR(100) NOT NULL,
    document_id VARCHAR(255) NOT NULL,
    content JSONB,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_organization_id ON jobs(organization_id);
CREATE INDEX IF NOT EXISTS idx_jobs_user_id ON jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_documents_job_id ON documents(job_id);