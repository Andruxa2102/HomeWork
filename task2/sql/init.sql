CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS nsi;

GRANT ALL PRIVILEGES ON SCHEMA nsi TO nsi_admin;

COMMENT ON SCHEMA nsi IS 'Схема для нормативно-справочной информации';

CREATE TABLE IF NOT EXISTS nsi.raw_medical_organizations (
    raw_org_hash_key VARCHAR(64) PRIMARY KEY,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    source_version VARCHAR(20),
    payload JSONB
);
COMMENT ON TABLE nsi.raw_medical_organizations IS 'Реестр медицинских и фармацевтических организаций Российской Федерации';


CREATE TYPE pipeline_statuses AS ENUM ('success', 'failed', 'unknown', 'started');
CREATE TABLE IF NOT EXISTS nsi.log_pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    source_identifier VARCHAR NOT NULL,    -- '1.2.643.5.1.13.13.11.1461'
    source_version VARCHAR,
    pipeline_status pipeline_statuses NOT NULL,
    records_affected INT DEFAULT 0,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT
);
COMMENT ON TABLE nsi.log_pipeline_runs IS 'Журнал логов';
-- Предполагаем добавление других ETL-процессов - поможет ускорить поиск последнего запуска
CREATE INDEX IF NOT EXISTS idx_log_pipeline_version ON nsi.log_pipeline_runs (source_identifier, pipeline_status, finished_at DESC);


CREATE TABLE IF NOT EXISTS nsi.hub_organization (
    hub_org_hash_key VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR,
    load_date TIMESTAMPTZ,
    record_source VARCHAR DEFAULT 'nsi.rosminzdrav.ru'
);
COMMENT ON TABLE nsi.hub_organization IS 'Хаб организаций';
CREATE INDEX IF NOT EXISTS idx_business_hub_organization ON nsi.hub_organization (org_id);


CREATE TABLE IF NOT EXISTS nsi.sat_organization_attrs (
    hub_org_hash_key VARCHAR(64),
    hashdiff VARCHAR(64),
    load_date TIMESTAMPTZ,
    full_name TEXT,
    short_name TEXT,
    ogrn VARCHAR,
    inn VARCHAR,
    address TEXT,
    ved_affiliation_id VARCHAR,
    inclusion_date DATE,
    record_source VARCHAR,
    PRIMARY KEY (hub_org_hash_key, load_date) 
);
COMMENT ON TABLE nsi.sat_organization_attrs IS 'Спутник основных атрибутов';
CREATE INDEX IF NOT EXISTS idx_sat_org_attrs_fk ON nsi.sat_organization_attrs (hub_org_hash_key);


CREATE TABLE IF NOT EXISTS nsi.sat_organization_changes (
    change_id SERIAL PRIMARY KEY,
    hub_org_hash_key VARCHAR(64),
    attribute_name VARCHAR,
    attribute_value TEXT,
    valid_from DATE,
    valid_to DATE,
    loaded_at TIMESTAMPTZ
);
COMMENT ON TABLE nsi.sat_organization_changes IS 'Спутник отслеживания изменений';
CREATE INDEX IF NOT EXISTS idx_sat_org_changes_fk ON nsi.sat_organization_changes (hub_org_hash_key);
