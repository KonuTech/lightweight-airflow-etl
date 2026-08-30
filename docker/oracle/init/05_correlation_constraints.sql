-- DB-level correlation safety net for CUSTOMERS_VALID / ORDERS_VALID (D-13, D-14,
-- D-15, D-18) -- a PRIMARY KEY on each `_valid` table's own id column, a plain
-- index on ORDERS_VALID.customer_id for the JOIN workload, and a BEFORE INSERT
-- trigger on ORDERS_VALID that rejects any row whose customer_id does not exist
-- in CUSTOMERS_VALID.
--
-- This is defense-in-depth on top of Plan 07-01's Python-side correlation
-- (shared rng/customer_id pool across the two generated datasets) -- it exists
-- to catch a FUTURE generator regression as a load failure instead of silent
-- bad data (D-14's explicit reasoning), never as a replacement for the Python
-- guarantee.
--
-- OUT OF SCOPE, deliberately (D-18): CUSTOMERS_INVALID / ORDERS_INVALID are
-- NEVER touched by this file -- no PK, no index, no trigger. Both stay fully
-- unconstrained so a malformed/empty customer_id or an orders_invalid row with
-- no matching customer can still be captured for inspection.
--
-- Every init script mounted under /container-entrypoint-initdb.d runs once, on first boot,
-- via a bare `/ as sysdba` connection that lands in CDB$ROOT as SYS -- NOT in FREEPDB1 as ADMIN.
-- These two ALTER SESSION statements are mandatory, first, every time, no exceptions.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;

-- D-14: PRIMARY KEY on customers_valid.customer_id. Its implicit unique index
-- already satisfies D-13's "plain index on customer_id" requirement on this
-- side (see 07-04-PLAN.md's <planner_decisions>) -- no separate explicit index
-- is added here, since it would be redundant with the PK's own index.
ALTER TABLE customers_valid ADD CONSTRAINT pk_customers_valid PRIMARY KEY (customer_id);

-- D-14: PRIMARY KEY on orders_valid.order_id (a different column than the FK/
-- join column below -- this table's PK does NOT imply an index on customer_id).
ALTER TABLE orders_valid ADD CONSTRAINT pk_orders_valid PRIMARY KEY (order_id);

-- D-13: the one genuinely NEW explicit index this plan adds -- orders_valid's
-- customer_id has no PK-implied index of its own, and this is the FK/join side
-- of the customers<->orders correlation.
CREATE INDEX ix_orders_valid_customer_id ON orders_valid (customer_id);

-- D-15/D-16: BEFORE INSERT trigger rejecting any orders_valid row whose
-- customer_id does not exist in customers_valid. On violation, Oracle's own
-- default executemany() array-DML behavior fails the WHOLE batch/chunk (D-16)
-- -- no partial insert.
--
-- NOTE: this trigger queries customers_valid (a DIFFERENT table than the one
-- it is defined on, orders_valid) from a BEFORE INSERT ... FOR EACH ROW body.
-- This is safe and does NOT raise Oracle's ORA-04091 mutating-table error --
-- that error only fires when a row-level trigger queries the SAME table it is
-- defined on. Cross-table lookups like this one are unaffected. Flagged here
-- explicitly (mirroring 04_widen_invalid_columns.sql's own ORA-01451 callout)
-- so a future maintainer does not "fix" a non-issue.
CREATE OR REPLACE TRIGGER trg_orders_valid_customer_exists
BEFORE INSERT ON orders_valid
FOR EACH ROW
DECLARE
  v_exists NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_exists FROM customers_valid WHERE customer_id = :NEW.customer_id;
  IF v_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'orders_valid.customer_id not found in customers_valid: ' || :NEW.customer_id);
  END IF;
END;
/
