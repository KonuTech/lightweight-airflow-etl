-- Customers data tables: CUSTOMERS_VALID / CUSTOMERS_INVALID (D-01, D-03).
-- Both use INTERVAL partitioning (daily) on INGESTED_AT so Oracle auto-creates each new
-- day's partition on first insert (D-03) -- no manual partition-maintenance job needed.
--
-- Every init script mounted under /container-entrypoint-initdb.d runs once, on first boot,
-- via a bare `/ as sysdba` connection that lands in CDB$ROOT as SYS -- NOT in FREEPDB1 as ADMIN.
-- These two ALTER SESSION statements are mandatory, first, every time, no exceptions.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;

CREATE TABLE customers_valid (
  customer_id      VARCHAR2(64)  NOT NULL,
  name             VARCHAR2(255) NOT NULL,
  country          VARCHAR2(64)  NOT NULL,
  birth_date       DATE,
  event_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
  signup_country   VARCHAR2(64),
  ingested_at      DATE          DEFAULT SYSDATE NOT NULL
)
PARTITION BY RANGE (ingested_at)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
( PARTITION p_initial VALUES LESS THAN (DATE '2020-01-01') );

CREATE TABLE customers_invalid (
  customer_id      VARCHAR2(64)  NOT NULL,
  name             VARCHAR2(255) NOT NULL,
  country          VARCHAR2(64)  NOT NULL,
  birth_date       DATE,
  event_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
  signup_country   VARCHAR2(64),
  ingested_at      DATE          DEFAULT SYSDATE NOT NULL,
  error_code       VARCHAR2(64)   NOT NULL,
  error_message    VARCHAR2(4000) NOT NULL,
  source_file      VARCHAR2(255)  NOT NULL,
  row_number       NUMBER         NOT NULL
)
PARTITION BY RANGE (ingested_at)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
( PARTITION p_initial VALUES LESS THAN (DATE '2020-01-01') );
