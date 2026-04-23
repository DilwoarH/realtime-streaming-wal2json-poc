# wal2json Prototype

A minimal CDC (Change Data Capture) prototype using **PostgreSQL 17**, the **wal2json** logical decoding plugin, and **Python**.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Docker Compose                                              │
│                                                              │
│  ┌─────────────┐    WAL (logical)    ┌──────────────────┐   │
│  │  PostgreSQL │ ─────────────────▶  │  consumer.py     │   │
│  │  + wal2json │                     │  (repl slot)     │   │
│  └──────┬──────┘                     └──────────────────┘   │
│         │ SQL (INSERT/UPDATE/DELETE)                         │
│  ┌──────┴──────┐                                            │
│  │ producer.py │                                            │
│  └─────────────┘                                            │
└──────────────────────────────────────────────────────────────┘
```

| Service    | Description |
|------------|-------------|
| `postgres` | PostgreSQL 17 with `wal2json` installed; `wal_level=logical` |
| `consumer` | Python script that opens a replication slot and prints WAL events |
| `producer` | Python script that generates a stream of DML changes (manual profile) |

## Quick start

### 1. Start PostgreSQL + consumer

```bash
docker compose up --build
```

The consumer will wait for Postgres to be healthy, create the `wal2json_slot` replication slot, then begin streaming.

### 2. Start the producer (separate terminal)

```bash
docker compose run --rm producer
```

The producer inserts orders, updates statuses, deletes cancelled orders, and adjusts product stock every 2 seconds. You will see colour-coded events appear in the consumer output:

```
--- BEGIN  xid=742  ts=2026-04-23T10:00:01.123Z ---
[INSERT] public.orders
  data     : {"id": 1, "customer": "Alice", "product": "Widget A", ...}
--- COMMIT ---

--- BEGIN  xid=743  ts=2026-04-23T10:00:03.456Z ---
[UPDATE] public.orders
  data     : {"id": 1, "customer": "Alice", "product": "Widget A", "status": "shipped", ...}
  identity : {"id": 1}
--- COMMIT ---
```

### 3. Tear down

```bash
docker compose down -v   # -v also removes the pg_data volume
```

## Configuration

All services read their settings from environment variables. Override them in `docker-compose.yml` or via `--env`:

| Variable          | Default         | Description                          |
|-------------------|-----------------|--------------------------------------|
| `PGHOST`          | `postgres`      | Postgres hostname                    |
| `PGPORT`          | `5432`          | Postgres port                        |
| `PGDATABASE`      | `testdb`        | Database name                        |
| `PGUSER`          | `postgres`      | Database user                        |
| `PGPASSWORD`      | `postgres`      | Database password                    |
| `SLOT_NAME`       | `wal2json_slot` | Logical replication slot name        |
| `CHANGE_INTERVAL` | `2`             | Seconds between producer operations  |

## Running the Python scripts locally (no Docker)

```bash
cd python
pip install -r requirements.txt

# terminal 1 – assumes Postgres is reachable on localhost:5432
PGHOST=localhost python consumer.py

# terminal 2
PGHOST=localhost python producer.py
```

## Project layout

```
.
├── docker-compose.yml
├── postgres/
│   ├── Dockerfile          # postgres:17 + postgresql-17-wal2json
│   └── init.sql            # creates orders and products tables
└── python/
    ├── Dockerfile
    ├── requirements.txt    # psycopg2-binary
    ├── consumer.py         # logical replication consumer
    └── producer.py         # DML workload generator
```

## References

- [wal2json](https://github.com/eulerto/wal2json) – output plugin for logical decoding
- [psycopg2 replication support](https://www.psycopg.org/docs/extras.html#replication-support-objects)
- [PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
