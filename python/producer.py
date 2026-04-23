"""
WAL Producer
------------
Continuously inserts, updates, and deletes rows in the 'orders' and 'products'
tables so that the consumer has a steady stream of WAL events to display.

Press Ctrl-C to stop.
"""

import os
import random
import time

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_CONFIG: dict = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "testdb"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

CHANGE_INTERVAL = float(os.environ.get("CHANGE_INTERVAL", "2"))  # seconds between ops

CUSTOMERS = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
PRODUCTS  = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Tool Z"]
STATUSES  = ["pending", "processing", "shipped", "delivered", "cancelled"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_postgres(retries: int = 30, delay: float = 2.0) -> None:
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


def seed_products(conn: psycopg2.extensions.connection) -> None:
    """Ensure every product row exists with an initial stock value."""
    with conn.cursor() as cur:
        for name in PRODUCTS:
            cur.execute(
                """
                INSERT INTO products (name, stock)
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING
                """,
                (name, random.randint(50, 200)),
            )
    conn.commit()
    print("Products seeded (or already present).")


# ---------------------------------------------------------------------------
# Individual operations
# ---------------------------------------------------------------------------

def op_insert_order(conn: psycopg2.extensions.connection) -> None:
    customer = random.choice(CUSTOMERS)
    product  = random.choice(PRODUCTS)
    qty      = random.randint(1, 10)
    price    = round(random.uniform(5.0, 150.0), 2)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders (customer, product, quantity, price)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (customer, product, qty, price),
        )
        order_id = cur.fetchone()[0]
    conn.commit()
    print(f"[INSERT] Order #{order_id}: {customer} | {qty}x {product} @ ${price:.2f}")


def op_update_order_status(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if row is None:
            return
        order_id   = row[0]
        new_status = random.choice(STATUSES)
        cur.execute(
            "UPDATE orders SET status = %s WHERE id = %s",
            (new_status, order_id),
        )
    conn.commit()
    print(f"[UPDATE] Order #{order_id} → status = '{new_status}'")


def op_delete_cancelled_order(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM orders
            WHERE id = (
                SELECT id FROM orders
                WHERE status = 'cancelled'
                ORDER BY random()
                LIMIT 1
            )
            RETURNING id
            """
        )
        row = cur.fetchone()
    conn.commit()
    if row:
        print(f"[DELETE] Removed cancelled order #{row[0]}")


def op_update_product_stock(conn: psycopg2.extensions.connection) -> None:
    product = random.choice(PRODUCTS)
    delta   = random.randint(-15, 30)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE products
            SET stock      = GREATEST(0, stock + %s),
                updated_at = NOW()
            WHERE name = %s
            """,
            (delta, product),
        )
    conn.commit()
    sign = "+" if delta >= 0 else ""
    print(f"[UPDATE] Product '{product}' stock {sign}{delta}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

OPERATIONS = [
    op_insert_order,
    op_update_order_status,
    op_delete_cancelled_order,
    op_update_product_stock,
]
WEIGHTS = [4, 3, 1, 2]  # inserts are most frequent


def main() -> None:
    wait_for_postgres()

    conn = psycopg2.connect(**DB_CONFIG)
    seed_products(conn)

    print(f"Producing a change every {CHANGE_INTERVAL}s … (Ctrl-C to stop)\n")
    try:
        while True:
            op = random.choices(OPERATIONS, weights=WEIGHTS, k=1)[0]
            try:
                op(conn)
            except psycopg2.Error as exc:
                print(f"[WARN] DB error: {exc!s}")
                conn.rollback()
            time.sleep(CHANGE_INTERVAL)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
