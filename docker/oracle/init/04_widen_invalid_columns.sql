-- Widen CUSTOMERS_INVALID / ORDERS_INVALID data columns to nullable VARCHAR2 at their
-- current declared size, plus a new RAW_LINE column (D-01, D-02, D-04, D-05, D-06).
--
-- ENGINE-06 requires invalid rows to carry their ORIGINAL field values -- a malformed date
-- string or empty required field cannot literally be inserted into the native NOT NULL/typed
-- columns these two tables currently declare (mirrored from CUSTOMERS_VALID/ORDERS_VALID).
-- This migration widens every DATA column (never error_code/error_message/source_file/
-- row_number, and never the _VALID tables) to nullable VARCHAR2, preserving each column's
-- current size exactly (D-04) -- an oversized original value is its own distinct, worth-
-- flagging error condition, not something to silently accommodate by widening to VARCHAR2(4000).
--
-- Every init script mounted under /container-entrypoint-initdb.d runs once, on first boot,
-- via a bare `/ as sysdba` connection that lands in CDB$ROOT as SYS -- NOT in FREEPDB1 as ADMIN.
-- These two ALTER SESSION statements are mandatory, first, every time, no exceptions.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;

-- CUSTOMERS_INVALID: widen every data column to nullable VARCHAR2 at its current size.
-- birth_date/event_ts change from DATE/TIMESTAMP WITH TIME ZONE to VARCHAR2(64) since D-01
-- requires storing the raw original string, never a converted/typed value.
--
-- Oracle's ALTER TABLE ... MODIFY raises ORA-01451 ("column to be modified to NULL cannot
-- be modified to NULL") if a column is explicitly given a `NULL` clause while it already
-- permits nulls -- so `birth_date` (already nullable DATE in 02_customers.sql) omits the
-- `NULL` keyword here and only changes type; `customer_id`/`name`/`country`/`event_ts` were
-- originally `NOT NULL`, so they need the explicit `NULL` clause to drop that constraint.
-- `signup_country` is already nullable VARCHAR2(64) -- no change needed, omitted entirely.
ALTER TABLE customers_invalid MODIFY (
  customer_id      VARCHAR2(64)  NULL,
  name             VARCHAR2(255) NULL,
  country          VARCHAR2(64)  NULL,
  birth_date       VARCHAR2(64),
  event_ts         VARCHAR2(64)  NULL
);

-- D-06: entire original CSV line, defense-in-depth for byte_level_hard corpus fixtures.
ALTER TABLE customers_invalid ADD (raw_line VARCHAR2(4000));

-- ORDERS_INVALID: identical treatment. order_date/amount are already nullable in
-- 03_orders.sql, so they omit the `NULL` keyword (type change only, per ORA-01451 above).
ALTER TABLE orders_invalid MODIFY (
  order_id       VARCHAR2(64) NULL,
  customer_id    VARCHAR2(64) NULL,
  order_date     VARCHAR2(64),
  amount         VARCHAR2(64)
);

ALTER TABLE orders_invalid ADD (raw_line VARCHAR2(4000));
