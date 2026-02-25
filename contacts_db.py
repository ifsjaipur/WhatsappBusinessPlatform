"""Contacts Database Layer (Mini CRM)

Manages contacts with lead pipeline stages, tags, AI toggle,
and bulk import/export for campaign targeting.
"""

import json
from datetime import datetime, timezone

import aiosqlite
from loguru import logger

from db import DB_PATH, _enable_foreign_keys, _validate_columns
from utils import generate_id

VALID_STAGES = ("new", "contacted", "interested", "enrolled", "lost")


async def init_contacts_table():
    """Create contacts table. Called from init_db()."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id          TEXT PRIMARY KEY,
                phone       TEXT UNIQUE NOT NULL,
                name        TEXT DEFAULT '',
                email       TEXT DEFAULT '',
                source      TEXT DEFAULT 'whatsapp',
                stage       TEXT DEFAULT 'new',
                tags        TEXT DEFAULT '[]',
                notes       TEXT DEFAULT '',
                ai_enabled  INTEGER DEFAULT 1,
                first_seen  TEXT DEFAULT (datetime('now')),
                last_seen   TEXT DEFAULT (datetime('now')),
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()
    logger.info("Contacts table initialized")


def _parse_contact(row: dict) -> dict:
    """Parse JSON fields in a contact row."""
    record = dict(row)
    if record.get("tags"):
        try:
            record["tags"] = json.loads(record["tags"])
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Contact {record.get('id', '?')}: failed to parse tags JSON")
    return record


async def get_or_create_contact(phone: str, name: str = "") -> dict:
    """Get existing contact by phone or create a new one (atomic upsert)."""
    now = datetime.now(timezone.utc).isoformat()
    contact_id = generate_id()

    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        db.row_factory = aiosqlite.Row

        # Atomic upsert: insert or update last_seen (and name if empty)
        await db.execute(
            """INSERT INTO contacts (id, phone, name, first_seen, last_seen)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                 last_seen = excluded.last_seen,
                 name = CASE WHEN contacts.name = '' OR contacts.name IS NULL
                             THEN excluded.name ELSE contacts.name END""",
            (contact_id, phone, name, now, now),
        )
        await db.commit()

        async with db.execute(
            "SELECT * FROM contacts WHERE phone = ?", (phone,)
        ) as cursor:
            row = await cursor.fetchone()
            return _parse_contact(row)


async def get_contact(contact_id: str) -> dict | None:
    """Get a single contact by ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE id = ?", (contact_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return _parse_contact(row)


async def get_contact_by_phone(phone: str) -> dict | None:
    """Get a single contact by phone number."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE phone = ?", (phone,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return _parse_contact(row)


_CONTACT_ALLOWED_COLUMNS = {
    "name", "email", "source", "stage", "tags", "notes", "ai_enabled",
    "last_seen",
}


async def update_contact(contact_id: str, **kwargs) -> dict | None:
    """Update contact fields. Only whitelisted columns are accepted."""
    if not kwargs:
        return await get_contact(contact_id)

    _validate_columns(kwargs, _CONTACT_ALLOWED_COLUMNS)

    # Validate stage if provided
    if "stage" in kwargs and kwargs["stage"] not in VALID_STAGES:
        raise ValueError(f"Invalid stage: {kwargs['stage']}. Must be one of {VALID_STAGES}")

    # JSON-encode tags if provided as list
    if "tags" in kwargs and isinstance(kwargs["tags"], list):
        kwargs["tags"] = json.dumps(kwargs["tags"], ensure_ascii=False)

    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [contact_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute(f"UPDATE contacts SET {set_clause} WHERE id = ?", values)
        await db.commit()
    return await get_contact(contact_id)


async def toggle_ai(contact_id: str, enabled: bool):
    """Toggle AI auto-reply for a contact."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE contacts SET ai_enabled = ? WHERE id = ?",
            (1 if enabled else 0, contact_id),
        )
        await db.commit()
    logger.info(f"Contact {contact_id}: AI {'enabled' if enabled else 'disabled'}")


async def search_contacts(query: str, limit: int = 50) -> list[dict]:
    """Search contacts by name or phone."""
    pattern = f"%{query}%"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM contacts WHERE name LIKE ? OR phone LIKE ? ORDER BY last_seen DESC LIMIT ?",
            (pattern, pattern, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [_parse_contact(row) for row in rows]


async def list_contacts(limit: int = 100, stage: str = "") -> list[dict]:
    """List contacts, optionally filtered by stage."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if stage and stage in VALID_STAGES:
            async with db.execute(
                "SELECT * FROM contacts WHERE stage = ? ORDER BY last_seen DESC LIMIT ?",
                (stage, limit),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM contacts ORDER BY last_seen DESC LIMIT ?",
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
        return [_parse_contact(row) for row in rows]


async def import_contacts(records: list[dict]) -> dict:
    """Bulk import contacts. Upserts by phone number using atomic INSERT ON CONFLICT.

    Args:
        records: List of dicts with keys: phone (required), name, email, tags, stage

    Returns:
        Dict with created_count and updated_count.
    """
    created = 0
    updated = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        try:
            for record in records:
                phone = record.get("phone", "").strip()
                if not phone:
                    skipped += 1
                    continue

                name = record.get("name", "").strip()
                email = record.get("email", "").strip()
                stage = record.get("stage", "").strip()
                tags = record.get("tags", "")

                # Validate stage if provided
                if stage and stage not in VALID_STAGES:
                    logger.warning(f"Import: invalid stage '{stage}' for {phone}, defaulting to 'new'")
                    stage = "new"

                if isinstance(tags, list):
                    tags = json.dumps(tags, ensure_ascii=False)
                elif isinstance(tags, str) and tags and not tags.startswith("["):
                    tags = json.dumps([t.strip() for t in tags.split(",") if t.strip()], ensure_ascii=False)
                elif not tags:
                    tags = "[]"

                # Atomic upsert using INSERT ON CONFLICT
                contact_id = generate_id()
                await db.execute(
                    """INSERT INTO contacts (id, phone, name, email, tags, source, stage, first_seen, last_seen)
                       VALUES (?, ?, ?, ?, ?, 'import', ?, ?, ?)
                       ON CONFLICT(phone) DO UPDATE SET
                         name = CASE WHEN excluded.name != '' THEN excluded.name ELSE contacts.name END,
                         email = CASE WHEN excluded.email != '' THEN excluded.email ELSE contacts.email END,
                         tags = CASE WHEN excluded.tags != '[]' THEN excluded.tags ELSE contacts.tags END,
                         stage = CASE WHEN excluded.stage != '' AND excluded.stage != 'new' THEN excluded.stage ELSE contacts.stage END,
                         last_seen = excluded.last_seen""",
                    (contact_id, phone, name, email, tags, stage or "new", now, now),
                )

                # Check if this was an insert or update
                if db.total_changes > 0:
                    async with db.execute(
                        "SELECT id FROM contacts WHERE phone = ?", (phone,)
                    ) as cursor:
                        row = await cursor.fetchone()
                        if row and row[0] == contact_id:
                            created += 1
                        else:
                            updated += 1

            await db.commit()
        except Exception:
            await db.rollback()
            raise

    logger.info(f"Contact import: {created} created, {updated} updated, {skipped} skipped")
    return {"created": created, "updated": updated, "skipped": skipped, "total": created + updated}


async def export_contacts(stage: str = "") -> list[dict]:
    """Export contacts as list of dicts (for CSV download)."""
    contacts = await list_contacts(limit=10000, stage=stage)
    # Flatten for CSV export
    for c in contacts:
        if isinstance(c.get("tags"), list):
            c["tags"] = ", ".join(c["tags"])
    return contacts


async def get_contact_stats() -> dict:
    """Get contact count per stage for CRM pipeline view."""
    async with aiosqlite.connect(DB_PATH) as db:
        stats = {}
        for stage in VALID_STAGES:
            async with db.execute(
                "SELECT COUNT(*) FROM contacts WHERE stage = ?", (stage,)
            ) as cursor:
                stats[stage] = (await cursor.fetchone())[0]

        async with db.execute("SELECT COUNT(*) FROM contacts") as cursor:
            stats["total"] = (await cursor.fetchone())[0]

        return stats
