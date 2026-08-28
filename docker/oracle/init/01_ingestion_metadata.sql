-- Ingestion metadata table: checksum-based idempotency guard (D-04).
-- Not partitioned (only the four data tables added in Plan 01-02 use INTERVAL partitioning).
--
-- Every init script mounted under /container-entrypoint-initdb.d runs once, on first boot,
-- via a bare `/ as sysdba` connection that lands in CDB$ROOT as SYS -- NOT in FREEPDB1 as ADMIN.
-- These two ALTER SESSION statements are mandatory, first, every time, no exceptions.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;

CREATE TABLE ingestion_metadata (
  id             NUMBER GENERATED ALWAYS AS IDENTITY,
  dataset        VARCHAR2(64) NOT NULL,
  file_name      VARCHAR2(255) NOT NULL,
  checksum       VARCHAR2(64) NOT NULL,
  processed_at   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  total_rows     NUMBER NOT NULL,
  valid_rows     NUMBER NOT NULL,
  invalid_rows   NUMBER NOT NULL,
  status         VARCHAR2(32) NOT NULL,
  CONSTRAINT pk_ingestion_metadata PRIMARY KEY (id),
  CONSTRAINT uq_ingestion_metadata_dataset_checksum UNIQUE (dataset, checksum)
);
