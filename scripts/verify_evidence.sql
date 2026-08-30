-- Reproducible Oracle evidence-capture script (D-09/D-10, TEST-03/DOC-01).
--
-- Read-only evidence only -- no UPDATE/INSERT/DELETE anywhere in this file.
-- Run against the live stack (`make up` first):
--   docker compose exec -T oracle sqlplus -s admin/admin@//localhost:1521/FREEPDB1 < scripts/verify_evidence.sql
--
-- Two result sets:
--   1. The latest ingestion_metadata record per dataset (row counts/status proof).
--   2. The customers-JOIN-orders business report (D-10): region proxy is
--      customers.country (no literal region column exists in this schema --
--      flagged substitution, not a silent assumption), grouped by
--      (country, month-of-orders.order_date). This is a read-only reporting
--      JOIN on customer_id -- it does NOT reopen the "orders.customer_id ->
--      customers.customer_id FK not enforced" out-of-scope decision
--      (PROJECT.md); no referential-integrity validation is performed here.

SET PAGESIZE 100
SET LINESIZE 200
SET FEEDBACK ON

COLUMN dataset FORMAT A12
COLUMN file_name FORMAT A30
COLUMN checksum FORMAT A20
COLUMN status FORMAT A28
COLUMN processed_at FORMAT A30

PROMPT ============================================================
PROMPT Latest ingestion per dataset (D-09/D-11a evidence)
PROMPT ============================================================
SELECT
    im.dataset,
    im.file_name,
    im.checksum,
    im.total_rows,
    im.valid_rows,
    im.invalid_rows,
    im.status,
    im.processed_at
FROM ingestion_metadata im
WHERE im.processed_at = (
    SELECT MAX(im2.processed_at)
    FROM ingestion_metadata im2
    WHERE im2.dataset = im.dataset
)
ORDER BY im.dataset;

COLUMN region FORMAT A20
COLUMN order_month FORMAT A12
COLUMN order_count FORMAT 999999
COLUMN total_amount FORMAT 999999999.99
COLUMN avg_amount FORMAT 9999999.99

PROMPT ============================================================
PROMPT customers JOIN orders business report (D-10): region proxy
PROMPT (customers.country) x month-of-order_date
PROMPT ============================================================
SELECT
    c.country AS region,
    TRUNC(o.order_date, 'MM') AS order_month,
    COUNT(*) AS order_count,
    SUM(o.amount) AS total_amount,
    ROUND(AVG(o.amount), 2) AS avg_amount
FROM customers_valid c
JOIN orders_valid o ON o.customer_id = c.customer_id
GROUP BY c.country, TRUNC(o.order_date, 'MM')
ORDER BY region, order_month;

EXIT;
