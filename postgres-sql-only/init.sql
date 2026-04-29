-- Tables for the wal2json prototype demo

CREATE TABLE IF NOT EXISTS orders (
    id         SERIAL PRIMARY KEY,
    customer   VARCHAR(100) NOT NULL,
    product    VARCHAR(100) NOT NULL,
    quantity   INTEGER      NOT NULL,
    price      NUMERIC(10, 2) NOT NULL,
    status     VARCHAR(20)  NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Full replica identity so wal2json includes ALL old column values
-- (including non-PK columns like 'status') on UPDATE and DELETE.
BEGIN;
SET lock_timeout = '30s';
SET statement_timeout = '60s';

-- Sets the replica identity for the orders table to use the primary key index.
-- This determines which columns are logged to the WAL (Write-Ahead Log) when rows are updated or deleted.
-- Using the primary key as replica identity ensures that logical replication can uniquely identify rows
-- for UPDATE and DELETE operations. This is essential for WAL-based replication tools like wal2json.
-- Without this, replication would need to log all columns to identify changed rows, increasing log size.
ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_pkey;
CREATE INDEX orders_status_idx ON orders(status); -- During normal index creation, table updates are blocked, but reads are still allowed.
ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_status_idx;

COMMIT;

-- CREATE REPLICATION SLOT wal2json_replication LOGICAL wal2json;
SELECT * FROM pg_create_logical_replication_slot('wal2json_replication', 'wal2json');

-- Example query to peek changes from the logical slot (run after making some changes to the 'orders' table):
-- SELECT * FROM pg_logical_slot_peek_changes('wal2json_replication', NULL, NULL, 'pretty-print', 'on', 'add-tables', 'public.orders');

