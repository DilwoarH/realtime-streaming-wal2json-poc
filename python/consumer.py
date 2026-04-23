"""
WAL Consumer
------------
Connects to PostgreSQL using a logical replication connection and streams
JSON-encoded change events produced by the wal2json output plugin.

Each line printed to stdout describes one DML change (INSERT / UPDATE / DELETE)
or transaction boundary (BEGIN / COMMIT).
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
# Message handler
# ---------------------------------------------------------------------------

def handle_message(msg: psycopg2.extras.ReplicationMessage) -> None:
    """Called for every message emitted by the replication slot."""
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
        label = f"--- {action}" + (f"  xid={xid}" if xid else "") + (f"  ts={ts}" if ts else "") + " ---"
        print(coloured(label, action))

    elif action in ("INSERT", "UPDATE", "DELETE"):
        schema = payload.get("schema", "public")
        table = payload.get("table", "?")
        fqn = f"{schema}.{table}"

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

    elif action == "MESSAGE":
        print(f"[MESSAGE] prefix={payload.get('prefix')} content={payload.get('content')!r}")

    # Acknowledge the message so the slot can advance
    msg.cursor.send_feedback(flush_lsn=msg.data_start)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    wait_for_postgres()

    print(f"Opening logical replication connection …")
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

    try:
        cur.consume_stream(handle_message)
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        cur.close()
        repl_conn.close()


if __name__ == "__main__":
    main()
