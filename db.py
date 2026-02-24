"""SQLite Database Layer for Call Records

Uses aiosqlite for async compatibility with the FastAPI/Pipecat event loop.
Database file is stored at data/calls.db.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from loguru import logger

DB_DIR = Path(__file__).parent / "data"
DB_PATH = DB_DIR / "calls.db"


async def init_db():
    """Create all tables. Called once at server startup."""
    DB_DIR.mkdir(exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS calls (
                id               TEXT PRIMARY KEY,
                caller_phone     TEXT,
                caller_name      TEXT,
                connected_at     TEXT,
                disconnected_at  TEXT,
                duration_seconds REAL,
                transcript       TEXT,
                recording_path   TEXT,
                handoff_requested INTEGER DEFAULT 0,
                handoff_reason   TEXT,
                topics           TEXT,
                status           TEXT DEFAULT 'completed',
                created_at       TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()

    # Initialize chat tables (conversations + messages)
    from chat_db import init_chat_tables
    await init_chat_tables()

    # Initialize contacts table
    from contacts_db import init_contacts_table
    await init_contacts_table()

    # Initialize campaign tables
    from campaign_db import init_campaign_tables
    await init_campaign_tables()

    # Initialize orders table
    from orders_db import init_orders_table
    await init_orders_table()

    # Run schema migrations for existing tables
    await _run_migrations(db_path=DB_PATH)

    logger.info(f"Database initialized at {DB_PATH}")


async def _run_migrations(db_path):
    """Add new columns to existing tables (safe if columns already exist)."""
    migrations = [
        ("conversations", "contact_id", "TEXT DEFAULT ''"),
        ("messages", "direction", "TEXT DEFAULT 'inbound'"),
        ("messages", "source", "TEXT DEFAULT 'ai'"),
        ("messages", "status", "TEXT DEFAULT ''"),
    ]
    async with aiosqlite.connect(db_path) as db:
        for table, column, col_type in migrations:
            try:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                logger.info(f"Migration: added {table}.{column}")
            except Exception:
                pass  # Column already exists
        await db.commit()


async def create_call_record(call_id: str, caller_phone: str, caller_name: str, connected_at: str):
    """Insert initial call record when call connects."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO calls (id, caller_phone, caller_name, connected_at) VALUES (?, ?, ?, ?)",
            (call_id, caller_phone, caller_name, connected_at),
        )
        await db.commit()
    logger.debug(f"Call {call_id}: DB record created for {caller_phone}")


async def complete_call_record(call_id: str, **kwargs):
    """Update call record when call ends. Accepts any column as keyword argument."""
    if not kwargs:
        return
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [call_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE calls SET {set_clause} WHERE id = ?", values)
        await db.commit()
    logger.debug(f"Call {call_id}: DB record updated")


async def get_call(call_id: str) -> dict | None:
    """Retrieve a single call record by ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM calls WHERE id = ?", (call_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            record = dict(row)
            # Parse JSON fields for API responses
            for field in ("transcript", "topics"):
                if record.get(field):
                    try:
                        record[field] = json.loads(record[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            return record


async def get_recent_calls(limit: int = 50) -> list[dict]:
    """Get recent calls for dashboard/monitoring."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM calls ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                record = dict(row)
                for field in ("transcript", "topics"):
                    if record.get(field):
                        try:
                            record[field] = json.loads(record[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
                results.append(record)
            return results


async def resolve_call(call_id: str):
    """Mark a call's handoff as resolved."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE calls SET status = 'resolved' WHERE id = ?",
            (call_id,),
        )
        await db.commit()
    logger.info(f"Call {call_id} resolved")


async def get_stats() -> dict:
    """Get aggregate stats for the dashboard."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Calls today
        async with db.execute(
            "SELECT COUNT(*) FROM calls WHERE date(created_at) = date('now')"
        ) as cursor:
            calls_today = (await cursor.fetchone())[0]

        # Avg call duration
        async with db.execute(
            "SELECT AVG(duration_seconds) FROM calls WHERE duration_seconds > 0"
        ) as cursor:
            avg_duration = (await cursor.fetchone())[0] or 0

        # Pending handoffs (calls)
        async with db.execute(
            "SELECT COUNT(*) FROM calls WHERE status = 'handoff_pending'"
        ) as cursor:
            call_handoffs = (await cursor.fetchone())[0]

        # Chats today
        async with db.execute(
            "SELECT COUNT(*) FROM conversations WHERE date(created_at) = date('now')"
        ) as cursor:
            chats_today = (await cursor.fetchone())[0]

        # Pending handoffs (chats)
        async with db.execute(
            "SELECT COUNT(*) FROM conversations WHERE status = 'handoff_pending'"
        ) as cursor:
            chat_handoffs = (await cursor.fetchone())[0]

        return {
            "calls_today": calls_today,
            "chats_today": chats_today,
            "pending_handoffs": call_handoffs + chat_handoffs,
            "avg_call_duration": round(avg_duration, 1),
        }
