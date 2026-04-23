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

CREATE TABLE products (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL UNIQUE,
    stock      INTEGER      NOT NULL DEFAULT 0,
    updated_at TIMESTAMP    NOT NULL DEFAULT NOW()
);
