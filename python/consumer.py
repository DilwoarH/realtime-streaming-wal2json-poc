"""
WAL Consumer
------------
Connects to PostgreSQL using a logical replication connection and streams
JSON-encoded change events produced by the wal2json output plugin.

Each line printed to stdout describes one DML change (INSERT / UPDATE / DELETE)
or transaction boundary (BEGIN / COMMIT).

Stats tracking
~~~~~~~~~~~~~~
The 'stats' table is kept up to date purely from the WAL stream:
  INSERT orders  → increment stats for the new status
  UPDATE orders  → decrement old status, increment new status (when changed)
  DELETE orders  → decrement stats for the deleted row's status

REPLICA IDENTITY FULL is required on the orders table so that wal2json
includes all old column values (not just the primary key) in the 'identity'
field of UPDATE and DELETE events.
"""

import json
import os
import time

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Configuration (all values can be overridden via environment variables)
# ---------------------------------------------------------------------------
DB_CONFIG: dict = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "testdb"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

SLOT_NAME = os.environ.get("SLOT_NAME", "wal2json_slot")

# wal2json options (format-version 2 emits one JSON object per row change)
PLUGIN_OPTIONS = {
    "format-version": "2",
    "include-xids": "1",
    "include-timestamp": "1",
    "include-schemas": "1",
    "include-types": "1",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ACTION_LABELS = {
    "I": "INSERT",
    "U": "UPDATE",
    "D": "DELETE",
    "B": "BEGIN",
    "C": "COMMIT",
    "M": "MESSAGE",
    "T": "TRUNCATE",
}

COLOURS = {
    "INSERT": "\033[92m",   # green
    "UPDATE": "\033[93m",   # yellow
    "DELETE": "\033[91m",   # red
    "BEGIN":  "\033[96m",   # cyan
    "COMMIT": "\033[96m",
    "RESET":  "\033[0m",
}


def coloured(text: str, action: str) -> str:
    colour = COLOURS.get(action, "")
    reset = COLOURS["RESET"] if colour else ""
    return f"{colour}{text}{reset}"


def wait_for_postgres(retries: int = 30, delay: float = 2.0) -> None:
    """Poll until PostgreSQL accepts connections."""
    print("Waiting for PostgreSQL to be ready …")
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("PostgreSQL is ready.\n")
            return
        except psycopg2.OperationalError as exc:
            print(f"  Attempt {attempt}/{retries}: {exc!s}")
            time.sleep(delay)
    raise RuntimeError("PostgreSQL did not become ready in time.")


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def adjust_stats(db_conn: psycopg2.extensions.connection, status: str, delta: int) -> None:
    """Increment or decrement the order_count for a given status."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stats (status, order_count, updated_at)
                 VALUES (%s, GREATEST(0, %s), NOW())
            ON CONFLICT (status) DO UPDATE
                SET order_count = GREATEST(0, stats.order_count + %s),
                    updated_at  = NOW()
            """,
            (status, max(0, delta), delta),
        )
    db_conn.commit()


def print_stats(db_conn: psycopg2.extensions.connection) -> None:
    """Print the current stats table to stdout."""
    with db_conn.cursor() as cur:
        cur.execute("SELECT status, order_count FROM stats ORDER BY status")
        rows = cur.fetchall()
    print("  [stats] " + "  ".join(f"{s}={n}" for s, n in rows))


# ---------------------------------------------------------------------------
# Message handler (factory – captures the regular DB connection)
# ---------------------------------------------------------------------------

def make_handler(db_conn: psycopg2.extensions.connection):
    """Return a message handler that uses db_conn to update the stats table."""

    def handle_message(msg: psycopg2.extras.ReplicationMessage) -> None:
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            print(f"[WARN] Could not parse payload: {msg.payload!r}")
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            return

        action_code = payload.get("action", "?")
        action = ACTION_LABELS.get(action_code, action_code)

        if action in ("BEGIN", "COMMIT"):
            xid = payload.get("xid", "")
            ts = payload.get("timestamp", "")
            label = (
                f"--- {action}"
                + (f"  xid={xid}" if xid else "")
                + (f"  ts={ts}" if ts else "")
                + " ---"
            )
            print(coloured(label, action))

        elif action in ("INSERT", "UPDATE", "DELETE"):
            schema = payload.get("schema", "public")
            table  = payload.get("table", "?")
            fqn    = f"{schema}.{table}"

            columns: dict = {}
            if "columns" in payload:
                columns = {c["name"]: c["value"] for c in payload["columns"]}

            identity: dict = {}
            if "identity" in payload:
                identity = {c["name"]: c["value"] for c in payload["identity"]}

            header = coloured(f"[{action}]", action) + f" {fqn}"
            print(header)
            if columns:
                print(f"  data     : {json.dumps(columns)}")
            if identity:
                print(f"  identity : {json.dumps(identity)}")

            # ------------------------------------------------------------------
            # Keep stats up to date from WAL events (orders table only)
            # ------------------------------------------------------------------
            if table == "orders":
                if action == "INSERT":
                    new_status = columns.get("status", "pending")
                    adjust_stats(db_conn, new_status, +1)
                    print_stats(db_conn)

                elif action == "UPDATE":
                    # REPLICA IDENTITY FULL → identity holds the full old row
                    old_status = identity.get("status")
                    new_status = columns.get("status")
                    if old_status and new_status and old_status != new_status:
                        adjust_stats(db_conn, old_status, -1)
                        adjust_stats(db_conn, new_status, +1)
                        print_stats(db_conn)

                elif action == "DELETE":
                    # REPLICA IDENTITY FULL → identity holds the full old row
                    old_status = identity.get("status")
                    if old_status:
                        adjust_stats(db_conn, old_status, -1)
                        print_stats(db_conn)

        elif action == "MESSAGE":
            print(f"[MESSAGE] prefix={payload.get('prefix')} content={payload.get('content')!r}")

        # Acknowledge the message so the slot can advance
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    return handle_message


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    wait_for_postgres()

    # Regular connection used for writing to the stats table
    print("Opening regular DB connection (for stats updates) …")
    db_conn = psycopg2.connect(**DB_CONFIG)

    print("Opening logical replication connection …")
    repl_conn = psycopg2.connect(
        **DB_CONFIG,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    cur = repl_conn.cursor()

    # Create the replication slot (idempotent)
    try:
        cur.create_replication_slot(SLOT_NAME, output_plugin="wal2json")
        print(f"Replication slot '{SLOT_NAME}' created.")
    except psycopg2.errors.DuplicateObject:
        print(f"Replication slot '{SLOT_NAME}' already exists.")

    print(f"Streaming changes from slot '{SLOT_NAME}' …")
    print("(Run the producer in another terminal / container to see events)\n")

    cur.start_replication(
        slot_name=SLOT_NAME,
        decode=True,
        options=PLUGIN_OPTIONS,
    )

    handler = make_handler(db_conn)

    try:
        cur.consume_stream(handler)
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        cur.close()
        repl_conn.close()
        db_conn.close()


if __name__ == "__main__":
    main()
