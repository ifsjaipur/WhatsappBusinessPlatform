"""Campaign Database Layer

Manages bulk template message campaigns with recipient tracking.
Each campaign targets a list of recipients and sends a WhatsApp
template message with rate-limited delivery.
"""

import json
from datetime import datetime, timezone

import aiosqlite
from loguru import logger

from db import DB_PATH, _enable_foreign_keys, _validate_columns
from utils import generate_id

VALID_CAMPAIGN_STATUSES = ("draft", "running", "paused", "completed", "failed")


async def init_campaign_tables():
    """Create campaigns and campaign_recipients tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id                TEXT PRIMARY KEY,
                name              TEXT NOT NULL,
                template_name     TEXT NOT NULL,
                template_category TEXT DEFAULT '',
                language          TEXT DEFAULT 'en',
                template_params   TEXT DEFAULT '[]',
                status            TEXT DEFAULT 'draft',
                recipient_count   INTEGER DEFAULT 0,
                sent_count        INTEGER DEFAULT 0,
                delivered_count   INTEGER DEFAULT 0,
                read_count        INTEGER DEFAULT 0,
                failed_count      INTEGER DEFAULT 0,
                rate_limit_per_min INTEGER DEFAULT 60,
                started_at        TEXT,
                completed_at      TEXT,
                created_at        TEXT DEFAULT (datetime('now'))
            )
        """)
        # Migration: add template_category column if missing
        try:
            await db.execute("ALTER TABLE campaigns ADD COLUMN template_category TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass  # Column already exists
        await db.execute("""
            CREATE TABLE IF NOT EXISTS campaign_recipients (
                id              TEXT PRIMARY KEY,
                campaign_id     TEXT NOT NULL,
                phone           TEXT NOT NULL,
                name            TEXT DEFAULT '',
                status          TEXT DEFAULT 'pending',
                wa_message_id   TEXT DEFAULT '',
                sent_at         TEXT,
                delivered_at    TEXT,
                read_at         TEXT,
                error_message   TEXT DEFAULT '',
                created_at      TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
                UNIQUE(campaign_id, phone)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipients_campaign
            ON campaign_recipients(campaign_id, status)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipients_wamid
            ON campaign_recipients(wa_message_id)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_campaigns_status
            ON campaigns(status)
        """)
        await db.commit()
    logger.info("Campaign tables initialized")


# --- Campaign CRUD ---


async def create_campaign(
    name: str,
    template_name: str,
    language: str = "en",
    template_category: str = "",
    template_params: list | None = None,
    rate_limit_per_min: int = 60,
) -> dict:
    """Create a new campaign in draft status."""
    campaign_id = generate_id()
    now = datetime.now(timezone.utc).isoformat()
    params_json = json.dumps(template_params or [], ensure_ascii=False)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO campaigns (id, name, template_name, template_category, language, template_params,
               rate_limit_per_min, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (campaign_id, name, template_name, template_category, language, params_json, rate_limit_per_min, now),
        )
        await db.commit()

    logger.info(f"Campaign created: {campaign_id} ({name})")
    return await get_campaign(campaign_id)


async def get_campaign(campaign_id: str) -> dict | None:
    """Get a single campaign by ID with recipient stats."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return _parse_campaign(row)


async def list_campaigns(limit: int = 100, status: str = "") -> list[dict]:
    """List campaigns, optionally filtered by status."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status and status in VALID_CAMPAIGN_STATUSES:
            async with db.execute(
                "SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM campaigns ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
        return [_parse_campaign(row) for row in rows]


_CAMPAIGN_ALLOWED_COLUMNS = {
    "name", "template_name", "template_category", "language", "template_params", "status",
    "recipient_count", "sent_count", "delivered_count", "read_count",
    "failed_count", "rate_limit_per_min", "started_at", "completed_at",
}


async def update_campaign(campaign_id: str, **kwargs) -> dict | None:
    """Update campaign fields. Only whitelisted columns are accepted."""
    if not kwargs:
        return await get_campaign(campaign_id)

    _validate_columns(kwargs, _CAMPAIGN_ALLOWED_COLUMNS)

    if "template_params" in kwargs and isinstance(kwargs["template_params"], list):
        kwargs["template_params"] = json.dumps(kwargs["template_params"], ensure_ascii=False)

    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [campaign_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute(f"UPDATE campaigns SET {set_clause} WHERE id = ?", values)
        await db.commit()
    return await get_campaign(campaign_id)


async def delete_campaign(campaign_id: str) -> bool:
    """Delete a campaign and its recipients. Only allowed for draft/completed/failed."""
    campaign = await get_campaign(campaign_id)
    if not campaign:
        return False
    if campaign["status"] == "running":
        raise ValueError("Cannot delete a running campaign. Pause it first.")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM campaign_recipients WHERE campaign_id = ?", (campaign_id,))
        await db.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
        await db.commit()
    logger.info(f"Campaign deleted: {campaign_id}")
    return True


# --- Recipients ---


async def add_recipients(campaign_id: str, records: list[dict]) -> dict:
    """Add recipients to a campaign. Deduplicates by phone within the campaign.

    Args:
        campaign_id: Campaign ID
        records: List of dicts with keys: phone (required), name (optional)

    Returns:
        Dict with added, duplicate, invalid counts.
    """
    added = 0
    duplicate = 0
    invalid = 0

    async with aiosqlite.connect(DB_PATH) as db:
        # Get existing phones for this campaign
        existing_phones = set()
        async with db.execute(
            "SELECT phone FROM campaign_recipients WHERE campaign_id = ?",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                existing_phones.add(row[0])

        for record in records:
            phone = str(record.get("phone", "")).strip()
            if not phone:
                invalid += 1
                continue
            # Normalize: remove +, spaces, dashes
            phone = phone.replace("+", "").replace(" ", "").replace("-", "")
            if not phone.isdigit() or len(phone) < 10:
                invalid += 1
                continue

            if phone in existing_phones:
                duplicate += 1
                continue

            name = str(record.get("name", "")).strip()
            recipient_id = generate_id()
            await db.execute(
                "INSERT INTO campaign_recipients (id, campaign_id, phone, name) VALUES (?, ?, ?, ?)",
                (recipient_id, campaign_id, phone, name),
            )
            existing_phones.add(phone)
            added += 1

        # Update recipient_count
        await db.execute(
            "UPDATE campaigns SET recipient_count = (SELECT COUNT(*) FROM campaign_recipients WHERE campaign_id = ?) WHERE id = ?",
            (campaign_id, campaign_id),
        )
        await db.commit()

    logger.info(f"Campaign {campaign_id}: added {added} recipients ({duplicate} dup, {invalid} invalid)")
    return {"added": added, "duplicate": duplicate, "invalid": invalid}


async def get_pending_recipients(campaign_id: str, limit: int = 100) -> list[dict]:
    """Get pending recipients ready to send."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM campaign_recipients WHERE campaign_id = ? AND status = 'pending' LIMIT ?",
            (campaign_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


_RECIPIENT_ALLOWED_COLUMNS = {
    "status", "wa_message_id", "error_message", "sent_at", "delivered_at", "read_at",
}


async def update_recipient_status(
    recipient_id: str,
    status: str,
    wa_message_id: str = "",
    error_message: str = "",
) -> None:
    """Update a recipient's delivery status."""
    now = datetime.now(timezone.utc).isoformat()
    updates = {"status": status}
    if wa_message_id:
        updates["wa_message_id"] = wa_message_id
    if error_message:
        updates["error_message"] = error_message
    if status == "sent":
        updates["sent_at"] = now
    elif status == "delivered":
        updates["delivered_at"] = now
    elif status == "read":
        updates["read_at"] = now

    _validate_columns(updates, _RECIPIENT_ALLOWED_COLUMNS)
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [recipient_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute(
            f"UPDATE campaign_recipients SET {set_clause} WHERE id = ?", values
        )
        await db.commit()


async def update_recipient_by_wamid(wa_message_id: str, status: str) -> bool:
    """Update recipient status by WhatsApp message ID (for delivery webhooks)."""
    if not wa_message_id:
        return False
    now = datetime.now(timezone.utc).isoformat()
    field_map = {"delivered": "delivered_at", "read": "read_at", "sent": "sent_at"}
    time_field = field_map.get(status)

    async with aiosqlite.connect(DB_PATH) as db:
        if time_field:
            await db.execute(
                f"UPDATE campaign_recipients SET status = ?, {time_field} = ? WHERE wa_message_id = ?",
                (status, now, wa_message_id),
            )
        else:
            await db.execute(
                "UPDATE campaign_recipients SET status = ? WHERE wa_message_id = ?",
                (status, wa_message_id),
            )
        changes = db.total_changes
        await db.commit()
    return changes > 0


async def refresh_campaign_stats(campaign_id: str) -> None:
    """Recount campaign stats from recipient rows using single GROUP BY query."""
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        counts = {"pending": 0, "sent": 0, "delivered": 0, "read": 0, "failed": 0}
        async with db.execute(
            "SELECT status, COUNT(*) FROM campaign_recipients WHERE campaign_id = ? GROUP BY status",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                if row[0] in counts:
                    counts[row[0]] = row[1]

        await db.execute(
            """UPDATE campaigns SET
               sent_count = ?, delivered_count = ?, read_count = ?, failed_count = ?
               WHERE id = ?""",
            (counts["sent"], counts["delivered"], counts["read"], counts["failed"], campaign_id),
        )
        await db.commit()


async def get_recipient_stats(campaign_id: str) -> dict:
    """Get count per status for a campaign using single GROUP BY query."""
    async with aiosqlite.connect(DB_PATH) as db:
        stats = {"pending": 0, "sent": 0, "delivered": 0, "read": 0, "failed": 0, "total": 0}
        async with db.execute(
            "SELECT status, COUNT(*) FROM campaign_recipients WHERE campaign_id = ? GROUP BY status",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                if row[0] in stats:
                    stats[row[0]] = row[1]
                stats["total"] += row[1]
        return stats


async def list_recipients(
    campaign_id: str,
    limit: int = 100,
    offset: int = 0,
    status: str = "",
) -> list[dict]:
    """List recipients for a campaign with optional status filter."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            async with db.execute(
                "SELECT * FROM campaign_recipients WHERE campaign_id = ? AND status = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
                (campaign_id, status, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM campaign_recipients WHERE campaign_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
                (campaign_id, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def export_campaign_results(campaign_id: str) -> list[dict]:
    """Export all recipients with their statuses for CSV download."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT phone, name, status, sent_at, delivered_at, read_at, error_message FROM campaign_recipients WHERE campaign_id = ? ORDER BY created_at ASC",
            (campaign_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


def _parse_campaign(row) -> dict:
    """Parse JSON fields in a campaign row."""
    record = dict(row)
    if record.get("template_params"):
        try:
            record["template_params"] = json.loads(record["template_params"])
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Campaign {record.get('id', '?')}: failed to parse template_params JSON")
    return record
