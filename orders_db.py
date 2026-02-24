"""Orders Database Layer

Manages WhatsApp catalog orders with Razorpay payment tracking.
Stores order items, payment status, and links to contacts.
"""

import json
from datetime import datetime, timezone

import aiosqlite
from loguru import logger

from db import DB_PATH
from utils import generate_id


async def init_orders_table():
    """Create orders table. Called from init_db()."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id                  TEXT PRIMARY KEY,
                contact_id          TEXT NOT NULL,
                conversation_id     TEXT DEFAULT '',
                phone               TEXT NOT NULL,
                name                TEXT DEFAULT '',
                catalog_id          TEXT DEFAULT '',
                items               TEXT NOT NULL DEFAULT '[]',
                total_amount        REAL DEFAULT 0,
                currency            TEXT DEFAULT 'INR',
                status              TEXT DEFAULT 'pending',
                razorpay_order_id   TEXT DEFAULT '',
                razorpay_payment_id TEXT DEFAULT '',
                razorpay_status     TEXT DEFAULT '',
                payment_link        TEXT DEFAULT '',
                notes               TEXT DEFAULT '',
                created_at          TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (contact_id) REFERENCES contacts(id)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_contact
            ON orders(contact_id)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_razorpay
            ON orders(razorpay_order_id)
        """)
        await db.commit()
    logger.info("Orders table initialized")


async def create_order(
    contact_id: str,
    phone: str,
    name: str = "",
    conversation_id: str = "",
    catalog_id: str = "",
    items: list[dict] | None = None,
    total_amount: float = 0,
    currency: str = "INR",
) -> dict:
    """Create a new order from an incoming WhatsApp order message."""
    order_id = generate_id()
    now = datetime.now(timezone.utc).isoformat()
    items_json = json.dumps(items or [], ensure_ascii=False)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO orders (id, contact_id, conversation_id, phone, name,
               catalog_id, items, total_amount, currency, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order_id, contact_id, conversation_id, phone, name,
             catalog_id, items_json, total_amount, currency, now, now),
        )
        await db.commit()

    logger.info(f"Order created: {order_id} for {phone} (amount: {total_amount} {currency})")
    return await get_order(order_id)


async def get_order(order_id: str) -> dict | None:
    """Get a single order by ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE id = ?", (order_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return _parse_order(row)


async def get_order_by_razorpay_id(razorpay_order_id: str) -> dict | None:
    """Get order by Razorpay order ID (for payment webhooks)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE razorpay_order_id = ?", (razorpay_order_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return _parse_order(row)


async def list_orders(limit: int = 100, status: str = "") -> list[dict]:
    """List orders, optionally filtered by status."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            async with db.execute(
                "SELECT * FROM orders WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
        return [_parse_order(row) for row in rows]


async def get_orders_by_contact(contact_id: str, limit: int = 20) -> list[dict]:
    """Get orders for a specific contact."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE contact_id = ? ORDER BY created_at DESC LIMIT ?",
            (contact_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [_parse_order(row) for row in rows]


async def update_order(order_id: str, **kwargs) -> dict | None:
    """Update order fields."""
    if not kwargs:
        return await get_order(order_id)

    if "items" in kwargs and isinstance(kwargs["items"], list):
        kwargs["items"] = json.dumps(kwargs["items"], ensure_ascii=False)

    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()

    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [order_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE orders SET {set_clause} WHERE id = ?", values)
        await db.commit()
    return await get_order(order_id)


async def set_razorpay_details(
    order_id: str,
    razorpay_order_id: str,
    payment_link: str = "",
) -> None:
    """Set Razorpay order ID and payment link after order creation."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE orders SET razorpay_order_id = ?, payment_link = ?,
               razorpay_status = 'created', status = 'payment_sent', updated_at = ?
               WHERE id = ?""",
            (razorpay_order_id, payment_link, now, order_id),
        )
        await db.commit()


async def mark_payment_completed(
    order_id: str,
    razorpay_payment_id: str,
) -> dict | None:
    """Mark order as paid after successful Razorpay payment."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE orders SET razorpay_payment_id = ?, razorpay_status = 'captured',
               status = 'payment_completed', updated_at = ?
               WHERE id = ?""",
            (razorpay_payment_id, now, order_id),
        )
        await db.commit()
    return await get_order(order_id)


async def mark_payment_failed(order_id: str, reason: str = "") -> None:
    """Mark order payment as failed."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE orders SET razorpay_status = 'failed', status = 'failed',
               notes = ?, updated_at = ? WHERE id = ?""",
            (reason[:500], now, order_id),
        )
        await db.commit()


async def get_order_stats() -> dict:
    """Get order counts by status."""
    async with aiosqlite.connect(DB_PATH) as db:
        stats = {}
        for status in ("pending", "payment_sent", "payment_completed", "failed"):
            async with db.execute(
                "SELECT COUNT(*) FROM orders WHERE status = ?", (status,)
            ) as cursor:
                stats[status] = (await cursor.fetchone())[0]

        async with db.execute("SELECT COUNT(*) FROM orders") as cursor:
            stats["total"] = (await cursor.fetchone())[0]

        async with db.execute(
            "SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE status = 'payment_completed'"
        ) as cursor:
            stats["total_revenue"] = (await cursor.fetchone())[0]

        return stats


def _parse_order(row) -> dict:
    """Parse JSON fields in an order row."""
    record = dict(row)
    if record.get("items"):
        try:
            record["items"] = json.loads(record["items"])
        except (json.JSONDecodeError, TypeError):
            pass
    return record
