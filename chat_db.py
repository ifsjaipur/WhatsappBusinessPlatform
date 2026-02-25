"""Database Layer for Text Conversations + Messages

Manages WhatsApp text chat sessions in SQLite. Each phone number gets one
active conversation at a time; conversations auto-expire after 24 hours of
inactivity (lazy check on lookup).
"""

import json
from datetime import datetime, timedelta, timezone

import aiosqlite
from loguru import logger

from db import DB_PATH, _enable_foreign_keys, _validate_columns
from utils import generate_id

# Dedup window: ignore duplicate wa_message_id only within this window
DEDUP_WINDOW_HOURS = 48

SESSION_TIMEOUT_HOURS = 24


async def init_chat_tables():
    """Create conversations and messages tables. Called from init_db()."""
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id              TEXT PRIMARY KEY,
                phone           TEXT NOT NULL,
                name            TEXT DEFAULT '',
                contact_id      TEXT DEFAULT '',
                started_at      TEXT DEFAULT (datetime('now')),
                last_message_at TEXT DEFAULT (datetime('now')),
                status          TEXT DEFAULT 'active',
                handoff_requested INTEGER DEFAULT 0,
                handoff_reason  TEXT DEFAULT '',
                topics          TEXT DEFAULT '[]',
                message_count   INTEGER DEFAULT 0,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id              TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                role            TEXT NOT NULL,
                content         TEXT NOT NULL,
                wa_message_id   TEXT DEFAULT '',
                direction       TEXT DEFAULT 'inbound',
                source          TEXT DEFAULT 'ai',
                status          TEXT DEFAULT '',
                created_at      TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_conversation
            ON messages(conversation_id)
        """)
        await db.commit()
    logger.info("Chat tables initialized")


async def get_or_create_conversation(phone: str, name: str = "", contact_id: str = "") -> dict:
    """Get active conversation for phone or create a new one.

    Expires conversations older than SESSION_TIMEOUT_HOURS.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=SESSION_TIMEOUT_HOURS)).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Expire old active conversations for this phone
        await db.execute(
            "UPDATE conversations SET status = 'expired' WHERE phone = ? AND status = 'active' AND last_message_at < ?",
            (phone, cutoff),
        )
        await db.commit()

        # Look for existing active conversation
        async with db.execute(
            "SELECT * FROM conversations WHERE phone = ? AND status IN ('active', 'handoff_pending') ORDER BY last_message_at DESC LIMIT 1",
            (phone,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                conv = dict(row)
                # Update contact_id if not set
                if contact_id and not conv.get("contact_id"):
                    await db.execute(
                        "UPDATE conversations SET contact_id = ? WHERE id = ?",
                        (contact_id, conv["id"]),
                    )
                    await db.commit()
                    conv["contact_id"] = contact_id
                return conv

        # Create new conversation
        conv_id = generate_id()
        now = datetime.now(timezone.utc).isoformat()
        await db.execute(
            "INSERT INTO conversations (id, phone, name, contact_id, started_at, last_message_at) VALUES (?, ?, ?, ?, ?, ?)",
            (conv_id, phone, name, contact_id, now, now),
        )
        await db.commit()
        logger.info(f"New conversation {conv_id} for {phone}")

        async with db.execute("SELECT * FROM conversations WHERE id = ?", (conv_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row)


async def add_message(
    conversation_id: str,
    role: str,
    content: str,
    wa_message_id: str = "",
    direction: str = "inbound",
    source: str = "ai",
) -> str:
    """Add a message to a conversation. Returns message ID."""
    msg_id = generate_id()
    now = datetime.now(timezone.utc).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO messages (id, conversation_id, role, content, wa_message_id, direction, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (msg_id, conversation_id, role, content, wa_message_id, direction, source, now),
        )
        await db.execute(
            "UPDATE conversations SET last_message_at = ?, message_count = message_count + 1 WHERE id = ?",
            (now, conversation_id),
        )
        await db.commit()
    return msg_id


async def check_duplicate_message(wa_message_id: str) -> bool:
    """Check if a WhatsApp message ID was already processed within the dedup window."""
    if not wa_message_id:
        return False
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=DEDUP_WINDOW_HOURS)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM messages WHERE wa_message_id = ? AND created_at > ? LIMIT 1",
            (wa_message_id, cutoff),
        ) as cursor:
            return await cursor.fetchone() is not None


async def get_recent_messages(conversation_id: str, limit: int = 10) -> list[dict]:
    """Get recent messages for a conversation (for LLM context window)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT role, content FROM messages WHERE conversation_id = ? ORDER BY created_at DESC LIMIT ?",
            (conversation_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            # Reverse so oldest is first (chronological order for LLM)
            return [dict(row) for row in reversed(rows)]


_CONVERSATION_ALLOWED_COLUMNS = {
    "phone", "name", "contact_id", "last_message_at", "status",
    "handoff_requested", "handoff_reason", "topics", "message_count",
}


async def update_conversation(conversation_id: str, **kwargs):
    """Update conversation fields. Only whitelisted columns are accepted."""
    if not kwargs:
        return
    _validate_columns(kwargs, _CONVERSATION_ALLOWED_COLUMNS)
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [conversation_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute(f"UPDATE conversations SET {set_clause} WHERE id = ?", values)
        await db.commit()


async def get_conversation(conversation_id: str) -> dict | None:
    """Get a single conversation with its messages."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute("SELECT * FROM conversations WHERE id = ?", (conversation_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            record = dict(row)
            for field in ("topics",):
                if record.get(field):
                    try:
                        record[field] = json.loads(record[field])
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse JSON field '{field}' in conversation {record.get('id', '?')}")

        # Fetch all messages
        async with db.execute(
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at ASC",
            (conversation_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            record["messages"] = [dict(r) for r in rows]

        return record


async def get_recent_conversations(limit: int = 50) -> list[dict]:
    """Get recent conversations for dashboard."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM conversations ORDER BY last_message_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                record = dict(row)
                for field in ("topics",):
                    if record.get(field):
                        try:
                            record[field] = json.loads(record[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
                results.append(record)
            return results


async def resolve_conversation(conversation_id: str):
    """Mark a conversation's handoff as resolved."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE conversations SET status = 'resolved', last_message_at = ? WHERE id = ?",
            (now, conversation_id),
        )
        await db.commit()
    logger.info(f"Conversation {conversation_id} resolved")


async def update_message_status(wa_message_id: str, status: str):
    """Update delivery status of an outbound message by WhatsApp message ID."""
    if not wa_message_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE messages SET status = ? WHERE wa_message_id = ?",
            (status, wa_message_id),
        )
        await db.commit()


async def get_inbox_conversations(limit: int = 50) -> list[dict]:
    """Get conversations for inbox view with last message preview."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM conversations WHERE status IN ('active', 'handoff_pending') ORDER BY last_message_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                record = dict(row)
                for field in ("topics",):
                    if record.get(field):
                        try:
                            record[field] = json.loads(record[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
                # Fetch last message preview
                async with db.execute(
                    "SELECT content, role, source FROM messages WHERE conversation_id = ? ORDER BY created_at DESC LIMIT 1",
                    (record["id"],),
                ) as msg_cursor:
                    msg_row = await msg_cursor.fetchone()
                    if msg_row:
                        record["last_message"] = dict(msg_row)
                    else:
                        record["last_message"] = None
                results.append(record)
            return results


async def get_conversation_messages(conversation_id: str, limit: int = 50, offset: int = 0) -> list[dict]:
    """Get paginated messages for a conversation (for inbox thread view)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (conversation_id, limit, offset),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
