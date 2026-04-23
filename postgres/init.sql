-- Tables for the wal2json prototype demo

CREATE TABLE orders (
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
ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TABLE products (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL UNIQUE,
    stock      INTEGER      NOT NULL DEFAULT 0,
    updated_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Stats table: tracks order counts per status.
-- Updated exclusively via WAL events streamed by wal2json.
CREATE TABLE stats (
    status      VARCHAR(20) PRIMARY KEY,
    order_count INTEGER     NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP   NOT NULL DEFAULT NOW()
);

INSERT INTO stats (status) VALUES
    ('pending'),
    ('processing'),
    ('shipped'),
    ('delivered'),
    ('cancelled');
