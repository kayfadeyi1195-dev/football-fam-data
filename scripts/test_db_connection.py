"""Quick smoke test for the database connection.

Connects to the database defined in .env, creates all tables if they
don't exist yet, and prints the table names to verify everything works.

Usage::

    python -m scripts.test_db_connection
"""

from sqlalchemy import text

from src.db.session import get_engine, get_session, init_db


def main() -> None:
    print("=" * 60)
    print("Football Fam — Database Connection Test")
    print("=" * 60)

    # Step 1 — verify raw connectivity
    engine = get_engine()
    print(f"\n1. Engine created  →  {engine.url}")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        pg_version = result.scalar()
        print(f"2. Connected OK   →  {pg_version}")

    # Step 2 — create tables (dev shortcut)
    print("\n3. Creating tables…")
    table_names = init_db()
    print(f"   {len(table_names)} tables found:")
    for name in sorted(table_names):
        print(f"      • {name}")

    # Step 3 — open a session and run a trivial query
    with get_session() as session:
        row_count = session.execute(text("SELECT 1")).scalar()
        print(f"\n4. Session test   →  SELECT 1 returned {row_count}")

    print("\n✓ All checks passed")
    print("=" * 60)


if __name__ == "__main__":
    main()
