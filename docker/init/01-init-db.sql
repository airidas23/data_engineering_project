-- Create schema
CREATE SCHEMA IF NOT EXISTS adform_dw;

-- Create the client report table with exact schema from requirements
CREATE TABLE IF NOT EXISTS adform_dw.client_report (
    datetime TIMESTAMP NOT NULL,
    impression_count BIGINT NOT NULL,
    click_count BIGINT NOT NULL,
    audit_loaded_datetime TIMESTAMP NOT NULL,
    PRIMARY KEY (datetime)
);

-- Create an index for performance
CREATE INDEX IF NOT EXISTS idx_client_report_datetime
ON adform_dw.client_report(datetime);

-- Create archive table with the same structure
CREATE TABLE IF NOT EXISTS adform_dw.client_report_archive (
    LIKE adform_dw.client_report INCLUDING ALL
);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON SCHEMA adform_dw TO adform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA adform_dw TO adform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA adform_dw
GRANT ALL PRIVILEGES ON TABLES TO adform_user;