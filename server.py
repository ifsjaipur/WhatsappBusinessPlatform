"""IFS WhatsApp Business Platform

FastAPI server that:
- Auto-accepts incoming WhatsApp calls -> Gemini Live AI voice agent
- Handles incoming WhatsApp text messages -> GPT-4o text chatbot
- Unified webhook for ALL WhatsApp events (text, media, orders, statuses)
- Inbox for manual replies with AI toggle per contact
- Mini CRM with lead pipeline, contact management, import/export
- Serves a password-protected dashboard with Inbox + Contacts + Knowledge editor

Receives webhooks forwarded from n8n, handles WebRTC via aiortc,
and posts call/chat summaries back to n8n for automation.
"""

import argparse
import asyncio
import os
import re
import secrets
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import (
    BackgroundTasks,
    Cookie,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.whatsapp.api import WhatsAppWebhookRequest
from pipecat.transports.whatsapp.client import WhatsAppClient

import media_storage
from bot import run_bot
from chat_db import (
    get_conversation,
    get_conversation_messages,
    get_inbox_conversations,
    get_recent_conversations,
    resolve_conversation,
    add_message,
    get_or_create_conversation,
)
from chatbot import handle_text_message
from contacts_db import (
    delete_contact,
    delete_contacts_bulk,
    export_contacts,
    get_contact,
    get_contact_by_phone,
    get_contact_stats,
    import_contacts,
    is_blocked,
    list_contacts,
    search_contacts,
    toggle_ai,
    update_contact,
)
from campaign_db import (
    add_recipients,
    create_campaign,
    delete_campaign,
    export_campaign_results,
    get_campaign,
    get_recipient_stats,
    list_campaigns,
    list_recipients,
    refresh_campaign_stats,
    update_campaign,
)
from campaign_runner import is_campaign_running, request_pause, run_campaign
from db import complete_call_record, delete_call, delete_calls_bulk, get_call, get_recent_calls, get_stats, init_db, resolve_call
from knowledge import KNOWLEDGE_DIR, load_knowledge
from message_router import route_webhook
from orders import handle_razorpay_webhook
from orders_db import get_order, get_order_stats, list_orders as list_orders_db
from whatsapp_messaging import (
    get_whatsapp_templates,
    send_interactive_message,
    send_whatsapp_template,
    send_whatsapp_text,
)

load_dotenv(override=True)

# Config
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_APP_SECRET = os.getenv("WHATSAPP_APP_SECRET", "")
WHATSAPP_WEBHOOK_VERIFICATION_TOKEN = os.getenv("WHATSAPP_WEBHOOK_VERIFICATION_TOKEN", "")
PORT = int(os.getenv("PORT", "7860"))

# Security config
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
SESSION_EXPIRY_HOURS = 24

IS_PRODUCTION = os.getenv("ENVIRONMENT", "").lower() in ("production", "prod")

if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
    missing = [v for v, val in [
        ("WHATSAPP_TOKEN", WHATSAPP_TOKEN),
        ("WHATSAPP_PHONE_NUMBER_ID", WHATSAPP_PHONE_NUMBER_ID),
    ] if not val]
    raise ValueError(f"Missing required env vars: {', '.join(missing)}")

if not WHATSAPP_WEBHOOK_VERIFICATION_TOKEN:
    logger.warning("WHATSAPP_WEBHOOK_VERIFICATION_TOKEN not set — webhook verification may fail")

if not DASHBOARD_PASSWORD:
    if IS_PRODUCTION:
        raise ValueError("DASHBOARD_PASSWORD must be set in production")
    logger.warning("DASHBOARD_PASSWORD not set — dashboard auth is DISABLED (dev mode)")

if not WEBHOOK_SECRET:
    if IS_PRODUCTION:
        raise ValueError("WEBHOOK_SECRET must be set in production")
    logger.warning("WEBHOOK_SECRET not set — webhook validation is DISABLED (dev mode)")

# Ensure directories exist
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
RECORDINGS_DIR = Path(__file__).parent / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)
KNOWLEDGE_DIR.mkdir(exist_ok=True)

# Global state
whatsapp_client: Optional[WhatsAppClient] = None
shutdown_event = asyncio.Event()
knowledge_context = ""

# In-memory session store: {token: expiry_datetime}
active_sessions: dict[str, datetime] = {}


# --- Auth helpers ---


def create_session() -> str:
    """Create a new session token and store it."""
    token = secrets.token_urlsafe(32)
    active_sessions[token] = datetime.now(timezone.utc) + timedelta(hours=SESSION_EXPIRY_HOURS)
    # Clean expired sessions
    now = datetime.now(timezone.utc)
    expired = [t for t, exp in active_sessions.items() if exp < now]
    for t in expired:
        del active_sessions[t]
    return token


def verify_session(token: str) -> bool:
    """Check if a session token is valid and not expired."""
    if not token or token not in active_sessions:
        return False
    if active_sessions[token] < datetime.now(timezone.utc):
        del active_sessions[token]
        return False
    return True


async def require_auth(
    session_token: str = Cookie(default=""),
    authorization: str = Header(default=""),
):
    """FastAPI dependency that requires a valid session cookie OR Bearer token.

    Supports two auth methods:
    - Cookie: session_token cookie (for dashboard browser sessions)
    - Bearer: Authorization: Bearer {DASHBOARD_PASSWORD} header (for n8n/API calls)

    If DASHBOARD_PASSWORD is not set, auth is disabled (dev mode).
    """
    if not DASHBOARD_PASSWORD:
        return  # Auth disabled in dev mode

    # Check session cookie first
    if verify_session(session_token):
        return

    # Check Bearer token (password-based for API access)
    if authorization.startswith("Bearer "):
        token = authorization[7:]
        if token == DASHBOARD_PASSWORD:
            return

    raise HTTPException(status_code=401, detail="Unauthorized")


async def require_auth_csrf(
    request: Request,
    session_token: str = Cookie(default=""),
    authorization: str = Header(default=""),
    csrf_token: str = Cookie(default=""),
):
    """Auth + CSRF validation for mutating endpoints (POST/PATCH/DELETE).

    CSRF check uses double-submit cookie: the X-CSRF-Token header must match
    the csrf_token cookie. Bearer token auth skips CSRF (API access).
    """
    if not DASHBOARD_PASSWORD:
        return  # Auth disabled in dev mode

    # Bearer token — no CSRF needed for API clients
    if authorization.startswith("Bearer "):
        token = authorization[7:]
        if token == DASHBOARD_PASSWORD:
            return

    # Session cookie auth
    if not verify_session(session_token):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # CSRF double-submit check for browser sessions
    if request.method in ("POST", "PATCH", "PUT", "DELETE"):
        header_csrf = request.headers.get("X-CSRF-Token", "")
        if not csrf_token or not header_csrf or csrf_token != header_csrf:
            raise HTTPException(status_code=403, detail="CSRF token mismatch")


# --- Knowledge file helpers ---


def validate_knowledge_filename(filename: str) -> str:
    """Validate and sanitize a knowledge filename. Returns sanitized name."""
    if not filename.endswith(".md"):
        raise HTTPException(status_code=400, detail="Filename must end with .md")
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not re.match(r'^[a-zA-Z0-9_\-]+\.md$', filename):
        raise HTTPException(status_code=400, detail="Filename can only contain letters, numbers, hyphens, and underscores")
    return filename


BACKGROUND_TASK_TIMEOUT = 300  # 5 minutes max for background tasks


async def _run_with_timeout(coro, timeout=BACKGROUND_TASK_TIMEOUT, task_name="background"):
    """Run an async coroutine with a timeout to prevent zombie tasks."""
    try:
        await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"{task_name} timed out after {timeout}s")
    except Exception as e:
        logger.error(f"{task_name} error: {e}")


def signal_handler():
    logger.info("Shutdown signal received")
    shutdown_event.set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global whatsapp_client, knowledge_context

    # Initialize database
    await init_db()

    # Initialize MinIO bucket + lifecycle rules
    media_storage.init_bucket()

    # Load knowledge docs at startup
    knowledge_context = load_knowledge()
    logger.info(f"Knowledge loaded: {len(knowledge_context)} characters")

    async with aiohttp.ClientSession() as session:
        whatsapp_client = WhatsAppClient(
            whatsapp_token=WHATSAPP_TOKEN,
            phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
            session=session,
        )
        logger.info("WhatsApp client initialized")
        try:
            yield
        finally:
            if whatsapp_client:
                await whatsapp_client.terminate_all_calls()
            logger.info("Cleanup done")


app = FastAPI(title="IFS WhatsApp Business Platform", version="4.0.0", lifespan=lifespan)

# Mount static files for dashboard (login page is always accessible,
# but dashboard.html checks auth via JS)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --- Auth Endpoints (no auth required) ---


@app.post("/auth/login")
async def auth_login(request: Request):
    """Login with dashboard password. Sets session cookie."""
    if not DASHBOARD_PASSWORD:
        return JSONResponse({"status": "ok", "message": "Auth disabled"})

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    password = body.get("password", "")
    if password != DASHBOARD_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")

    token = create_session()
    csrf_token = secrets.token_urlsafe(32)
    response = JSONResponse({"status": "ok", "csrf_token": csrf_token})
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=SESSION_EXPIRY_HOURS * 3600,
    )
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,  # JS needs to read this
        secure=True,
        samesite="strict",
        max_age=SESSION_EXPIRY_HOURS * 3600,
    )
    logger.info("Dashboard login successful")
    return response


@app.post("/auth/logout")
async def auth_logout(session_token: str = Cookie(default="")):
    """Logout and clear session."""
    if session_token in active_sessions:
        del active_sessions[session_token]
    response = JSONResponse({"status": "ok"})
    response.delete_cookie("session_token")
    return response


@app.get("/auth/check")
async def auth_check(session_token: str = Cookie(default="")):
    """Check if current session is valid."""
    if not DASHBOARD_PASSWORD:
        return {"authenticated": True, "auth_required": False}
    if verify_session(session_token):
        return {"authenticated": True, "auth_required": True}
    return JSONResponse({"authenticated": False, "auth_required": True}, status_code=401)


# --- WhatsApp Voice Call Webhooks (no auth — must be open for WhatsApp) ---


@app.get("/")
async def verify_webhook(request: Request):
    """WhatsApp webhook verification (GET)."""
    params = dict(request.query_params)
    try:
        result = await whatsapp_client.handle_verify_webhook_request(
            params=params,
            expected_verification_token=WHATSAPP_WEBHOOK_VERIFICATION_TOKEN,
        )
        logger.info("Webhook verified")
        return result
    except ValueError as e:
        logger.warning(f"Webhook verification failed: {e}")
        raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/")
async def handle_webhook(body: WhatsAppWebhookRequest, background_tasks: BackgroundTasks):
    """Handle incoming WhatsApp call webhooks from n8n.

    Auto-accepts every inbound call and spawns an AI bot session.
    """
    if body.object != "whatsapp_business_account":
        raise HTTPException(status_code=400, detail="Invalid object type")

    logger.info(f"Webhook received: {body.dict()}")

    # Extract caller info from webhook payload
    caller_phone = ""
    caller_name = ""
    try:
        for entry in body.entry:
            for change in entry.changes:
                value = change.value
                if hasattr(value, "contacts") and value.contacts:
                    contact = value.contacts[0]
                    caller_phone = contact.wa_id
                    if hasattr(contact, "profile") and contact.profile:
                        caller_name = contact.profile.name
                    break
            if caller_phone:
                break
    except Exception as e:
        logger.warning(f"Could not extract caller info: {e}")

    logger.info(f"Caller: {caller_phone} ({caller_name})")

    # Reload knowledge on each call (allows updating docs without restart)
    current_knowledge = load_knowledge()

    async def connection_callback(connection: SmallWebRTCConnection):
        try:
            logger.info(f"Auto-accepted call, starting AI bot: {connection.pc_id}")
            background_tasks.add_task(
                run_bot, connection, current_knowledge, caller_phone, caller_name
            )
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            try:
                await connection.disconnect()
            except Exception:
                pass

    try:
        result = await whatsapp_client.handle_webhook_request(body, connection_callback)
        return {"status": "success"}
    except ValueError as ve:
        logger.warning(f"Invalid webhook: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")


# --- WhatsApp Text Message Webhook (validated by secret, not session auth) ---


@app.post("/webhook/text")
async def handle_text_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle incoming WhatsApp text message webhooks from n8n.

    Validates WEBHOOK_SECRET if configured. Expects raw WhatsApp webhook JSON.
    """
    # Validate webhook secret if configured
    if WEBHOOK_SECRET:
        header_secret = request.headers.get("X-Webhook-Secret", "")
        query_secret = request.query_params.get("secret", "")
        if header_secret != WEBHOOK_SECRET and query_secret != WEBHOOK_SECRET:
            logger.warning("Text webhook rejected: invalid secret")
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logger.info("Text webhook received")

    # Extract sender info and message from WhatsApp webhook format
    sender_phone = ""
    sender_name = ""
    message_text = ""
    wa_message_id = ""

    try:
        entries = body.get("entry", [])
        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})

                # Get sender contact info
                contacts = value.get("contacts", [])
                if contacts:
                    sender_phone = contacts[0].get("wa_id", "")
                    profile = contacts[0].get("profile", {})
                    sender_name = profile.get("name", "")

                # Get message text
                messages = value.get("messages", [])
                if messages:
                    msg = messages[0]
                    wa_message_id = msg.get("id", "")
                    sender_phone = sender_phone or msg.get("from", "")

                    if msg.get("type") == "text":
                        message_text = msg.get("text", {}).get("body", "")

                if message_text:
                    break
            if message_text:
                break
    except Exception as e:
        logger.error(f"Failed to parse text webhook: {e}")
        raise HTTPException(status_code=400, detail="Could not parse message")

    if not message_text or not sender_phone:
        logger.warning("No text message found in webhook payload")
        return {"status": "skipped", "reason": "no text message"}

    logger.info(f"Text from {sender_phone} ({sender_name}): {message_text[:100]}")

    # Reload knowledge (allows live updates)
    current_knowledge = load_knowledge()

    # Handle message in background with timeout
    background_tasks.add_task(
        _run_with_timeout,
        handle_text_message(
            sender_phone, sender_name, message_text,
            wa_message_id, current_knowledge,
        ),
        BACKGROUND_TASK_TIMEOUT,
        f"text_message({sender_phone})",
    )

    return {"status": "success"}


# --- Unified WhatsApp Webhook (replaces separate call/text endpoints) ---


@app.post("/webhook/whatsapp")
async def handle_unified_webhook(request: Request, background_tasks: BackgroundTasks):
    """Unified webhook for ALL WhatsApp events from n8n.

    Routes events internally:
    - text → AI chatbot or manual inbox
    - media → store and acknowledge
    - order → store and acknowledge (Phase 4C: Razorpay)
    - interactive → store reply
    - statuses → delivery tracking
    - calls → forward to voice agent

    Validates WEBHOOK_SECRET if configured.
    """
    # Validate webhook secret if configured
    if WEBHOOK_SECRET:
        header_secret = request.headers.get("X-Webhook-Secret", "")
        query_secret = request.query_params.get("secret", "")
        if header_secret != WEBHOOK_SECRET and query_secret != WEBHOOK_SECRET:
            logger.warning("Unified webhook rejected: invalid secret")
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logger.info("Unified webhook received")

    # Check if this is a call event — delegate to voice handler
    is_call = False
    try:
        for entry in body.get("entry", []):
            for change in entry.get("changes", []):
                if change.get("field") == "calls":
                    is_call = True
                    break
            if is_call:
                break
    except Exception:
        pass

    if is_call:
        # Forward to Pipecat voice handler
        try:
            webhook_request = WhatsAppWebhookRequest(**body)
            return await handle_webhook(webhook_request, background_tasks)
        except Exception as e:
            logger.error(f"Failed to forward call to voice handler: {e}")
            return {"status": "error", "reason": "call forwarding failed"}

    # Reload knowledge for AI chatbot
    current_knowledge = load_knowledge()

    # Route via message router (runs in background with timeout)
    async def _route():
        result = await route_webhook(body, current_knowledge)
        logger.info(f"Router result: {result}")

    background_tasks.add_task(
        _run_with_timeout, _route(), BACKGROUND_TASK_TIMEOUT, "unified_webhook_route",
    )
    return {"status": "success"}


# --- Protected: Inbox API ---


@app.get("/api/inbox", dependencies=[Depends(require_auth)])
async def get_inbox(limit: int = 50):
    """List conversations for inbox view (active + handoff_pending)."""
    conversations = await get_inbox_conversations(limit)
    return {"conversations": conversations, "count": len(conversations)}


@app.get("/api/inbox/{conversation_id}/messages", dependencies=[Depends(require_auth)])
async def get_inbox_messages(conversation_id: str, limit: int = 50, offset: int = 0):
    """Get paginated messages for a conversation thread with presigned media URLs."""
    messages = await get_conversation_messages(conversation_id, limit, offset)

    # Enrich messages that have media_key with presigned URLs
    for msg in messages:
        media_key = msg.get("media_key", "")
        if media_key and media_storage.is_configured():
            msg["media_url"] = media_storage.generate_presigned_url(media_key)
        else:
            msg["media_url"] = None

    return {"messages": messages, "count": len(messages)}


@app.post("/api/messages/send", dependencies=[Depends(require_auth_csrf)])
async def send_manual_message(request: Request):
    """Send a manual reply from the inbox.

    Body: { "conversation_id": "...", "message": "..." }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    conversation_id = body.get("conversation_id", "").strip()
    message_text = body.get("message", "").strip()

    if not conversation_id or not message_text:
        raise HTTPException(status_code=400, detail="conversation_id and message are required")

    # Get conversation to find phone number
    conv = await get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    phone = conv["phone"]

    # Send via WhatsApp
    sent = await send_whatsapp_text(phone, message_text)
    if not sent:
        raise HTTPException(status_code=502, detail="Failed to send WhatsApp message")

    # Store in database
    msg_id = await add_message(
        conversation_id, "assistant", message_text,
        direction="outbound", source="manual",
    )

    logger.info(f"Manual reply sent to {phone} in conv {conversation_id}")
    return {"status": "sent", "message_id": msg_id, "phone": phone}


@app.post("/api/messages/send-direct", dependencies=[Depends(require_auth_csrf)])
async def send_direct_message(request: Request):
    """Send a message to a phone number directly (e.g. from Calls panel).

    Auto-creates or finds an existing conversation for the phone number.
    Body: { "phone": "919876543210", "name": "John", "message": "..." }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    phone = body.get("phone", "").strip()
    message_text = body.get("message", "").strip()
    name = body.get("name", "").strip()

    if not phone or not message_text:
        raise HTTPException(status_code=400, detail="phone and message are required")

    # Send via WhatsApp first
    sent = await send_whatsapp_text(phone, message_text)
    if not sent:
        raise HTTPException(status_code=502, detail="Failed to send WhatsApp message")

    # Get or create a conversation for this phone
    conv = await get_or_create_conversation(phone, name)

    # Store the message in the conversation
    msg_id = await add_message(
        conv["id"], "assistant", message_text,
        direction="outbound", source="manual",
    )

    logger.info(f"Direct message sent to {phone} (conv {conv['id']})")
    return {
        "status": "sent",
        "message_id": msg_id,
        "phone": phone,
        "conversation_id": conv["id"],
    }


@app.post("/api/messages/send-template", dependencies=[Depends(require_auth_csrf)])
async def api_send_template_direct(request: Request):
    """Send a template message to a phone number.

    Body: { "to": "919876543210", "template_name": "hello_world", "language": "en", "components": [...] }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    to_phone = body.get("to", "").strip()
    template_name = body.get("template_name", "").strip()
    if not to_phone or not template_name:
        raise HTTPException(status_code=400, detail="to and template_name are required")

    result = await send_whatsapp_template(
        to_phone=to_phone,
        template_name=template_name,
        language=body.get("language", "en"),
        components=body.get("components"),
    )
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Send failed"))


# --- Protected: Contacts API (Mini CRM) ---


@app.get("/api/contacts", dependencies=[Depends(require_auth)])
async def api_list_contacts(
    limit: int = 100, stage: str = "", search: str = "",
    sort: str = "last_seen", order: str = "desc",
):
    """List contacts with optional stage filter, search, and sort."""
    if search:
        contacts = await search_contacts(search, limit)
    else:
        contacts = await list_contacts(limit, stage, sort=sort, order=order)
    return {"contacts": contacts, "count": len(contacts)}


@app.get("/api/contacts/stats", dependencies=[Depends(require_auth)])
async def api_contact_stats():
    """Get contact count per pipeline stage."""
    return await get_contact_stats()


@app.get("/api/contacts/export", dependencies=[Depends(require_auth)])
async def api_export_contacts(stage: str = ""):
    """Export contacts as JSON (dashboard converts to CSV)."""
    contacts = await export_contacts(stage)
    return {"contacts": contacts, "count": len(contacts)}


@app.post("/api/contacts/import", dependencies=[Depends(require_auth_csrf)])
async def api_import_contacts(request: Request):
    """Bulk import contacts.

    Body: { "records": [{"phone": "...", "name": "...", "email": "...", "tags": "..."}, ...] }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    records = body.get("records", [])
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    result = await import_contacts(records)
    logger.info(f"Contact import: {result}")
    return result


@app.get("/api/contacts/{contact_id}", dependencies=[Depends(require_auth)])
async def api_get_contact(contact_id: str):
    """Get a single contact by ID."""
    contact = await get_contact(contact_id)
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    return contact


@app.patch("/api/contacts/{contact_id}", dependencies=[Depends(require_auth_csrf)])
async def api_update_contact(contact_id: str, request: Request):
    """Update contact fields (name, email, stage, tags, notes).

    Body: { "name": "...", "stage": "interested", ... }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Whitelist allowed fields
    allowed = {"name", "email", "stage", "tags", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed}

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    try:
        contact = await update_contact(contact_id, **updates)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    return contact


@app.patch("/api/contacts/{contact_id}/ai", dependencies=[Depends(require_auth_csrf)])
async def api_toggle_ai(contact_id: str, request: Request):
    """Toggle AI auto-reply for a contact.

    Body: { "enabled": true/false }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    enabled = body.get("enabled")
    if enabled is None:
        raise HTTPException(status_code=400, detail="'enabled' field required")

    contact = await get_contact(contact_id)
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    await toggle_ai(contact_id, bool(enabled))
    return {"status": "ok", "contact_id": contact_id, "ai_enabled": bool(enabled)}


@app.delete("/api/contacts/{contact_id}", dependencies=[Depends(require_auth_csrf)])
async def api_delete_contact(contact_id: str):
    """Delete a single contact."""
    deleted = await delete_contact(contact_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Contact not found")
    return {"status": "deleted", "contact_id": contact_id}


@app.post("/api/contacts/bulk-delete", dependencies=[Depends(require_auth_csrf)])
async def api_bulk_delete_contacts(request: Request):
    """Delete multiple contacts by ID.

    Body: { "ids": ["contact_id_1", "contact_id_2", ...] }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="No contact IDs provided")

    count = await delete_contacts_bulk(ids)
    return {"status": "deleted", "count": count}


# --- n8n Integration API ---


@app.get("/api/contacts/by-phone/{phone}", dependencies=[Depends(require_auth)])
async def api_contact_by_phone(phone: str):
    """Get contact summary by phone number (for n8n lookups)."""
    contact = await get_contact_by_phone(phone)
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    return contact


# --- Protected: Campaigns API ---


@app.get("/api/campaigns", dependencies=[Depends(require_auth)])
async def api_list_campaigns(limit: int = 100, status: str = ""):
    """List campaigns with optional status filter."""
    campaigns = await list_campaigns(limit, status)
    return {"campaigns": campaigns, "count": len(campaigns)}


@app.post("/api/campaigns", dependencies=[Depends(require_auth_csrf)])
async def api_create_campaign(request: Request):
    """Create a new campaign.

    Body: { "name": "...", "template_name": "...", "language": "en", "rate_limit_per_min": 60 }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    name = body.get("name", "").strip()
    template_name = body.get("template_name", "").strip()
    if not name or not template_name:
        raise HTTPException(status_code=400, detail="name and template_name are required")

    campaign = await create_campaign(
        name=name,
        template_name=template_name,
        language=body.get("language", "en"),
        template_category=body.get("template_category", ""),
        template_params=body.get("template_params"),
        rate_limit_per_min=int(body.get("rate_limit_per_min", 60)),
    )
    return campaign


@app.get("/api/campaigns/{campaign_id}", dependencies=[Depends(require_auth)])
async def api_get_campaign(campaign_id: str):
    """Get campaign detail with recipient stats."""
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    campaign["recipient_stats"] = await get_recipient_stats(campaign_id)
    return campaign


@app.patch("/api/campaigns/{campaign_id}", dependencies=[Depends(require_auth_csrf)])
async def api_update_campaign(campaign_id: str, request: Request):
    """Update campaign fields (name, rate_limit_per_min, template_params)."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    allowed = {"name", "rate_limit_per_min", "template_params"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    campaign = await update_campaign(campaign_id, **updates)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@app.delete("/api/campaigns/{campaign_id}", dependencies=[Depends(require_auth_csrf)])
async def api_delete_campaign(campaign_id: str):
    """Delete a campaign (not allowed while running)."""
    try:
        deleted = await delete_campaign(campaign_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"status": "deleted", "campaign_id": campaign_id}


@app.post("/api/campaigns/{campaign_id}/recipients", dependencies=[Depends(require_auth_csrf)])
async def api_add_recipients(campaign_id: str, request: Request):
    """Add recipients to a campaign.

    Body: { "recipients": [{"phone": "...", "name": "..."}, ...] }
    """
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if campaign["status"] == "running":
        raise HTTPException(status_code=400, detail="Cannot add recipients while campaign is running")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    records = body.get("recipients", [])
    if not records:
        raise HTTPException(status_code=400, detail="No recipients provided")

    result = await add_recipients(campaign_id, records)
    return result


@app.get("/api/campaigns/{campaign_id}/recipients", dependencies=[Depends(require_auth)])
async def api_list_recipients(campaign_id: str, limit: int = 100, offset: int = 0, status: str = ""):
    """List recipients for a campaign."""
    recipients = await list_recipients(campaign_id, limit, offset, status)
    return {"recipients": recipients, "count": len(recipients)}


@app.post("/api/campaigns/{campaign_id}/start", dependencies=[Depends(require_auth_csrf)])
async def api_start_campaign(campaign_id: str, background_tasks: BackgroundTasks):
    """Start sending a campaign."""
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if campaign["status"] not in ("draft", "paused"):
        raise HTTPException(status_code=400, detail=f"Cannot start campaign with status '{campaign['status']}'")
    if is_campaign_running(campaign_id):
        raise HTTPException(status_code=400, detail="Campaign is already running")

    stats = await get_recipient_stats(campaign_id)
    if stats["total"] == 0:
        raise HTTPException(status_code=400, detail="No recipients added to campaign")

    background_tasks.add_task(run_campaign, campaign_id)
    return {"status": "starting", "campaign_id": campaign_id}


@app.post("/api/campaigns/{campaign_id}/pause", dependencies=[Depends(require_auth_csrf)])
async def api_pause_campaign(campaign_id: str):
    """Pause a running campaign."""
    if not is_campaign_running(campaign_id):
        raise HTTPException(status_code=400, detail="Campaign is not running")
    request_pause(campaign_id)
    return {"status": "pausing", "campaign_id": campaign_id}


@app.get("/api/campaigns/{campaign_id}/results", dependencies=[Depends(require_auth)])
async def api_campaign_results(campaign_id: str):
    """Export campaign results for CSV download."""
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    results = await export_campaign_results(campaign_id)
    return {"campaign_id": campaign_id, "results": results, "count": len(results)}


@app.get("/api/templates", dependencies=[Depends(require_auth)])
async def api_list_templates():
    """Fetch WhatsApp message templates from Meta with full details."""
    templates = await get_whatsapp_templates()
    return {"templates": templates, "count": len(templates)}


@app.post("/api/whatsapp/send-interactive", dependencies=[Depends(require_auth_csrf)])
async def api_send_interactive(request: Request):
    """Send an interactive message (buttons or list) to a WhatsApp user.

    Body: {
        "to": "919876543210",
        "type": "button" | "list",
        "body": "Choose an option:",
        "buttons": [{"id": "btn_1", "title": "Option 1"}, ...],  // for type=button
        "sections": [{"title": "Section", "rows": [...]}],        // for type=list
        "header": {"type": "text", "text": "Header"},             // optional
        "footer": "Footer text"                                    // optional
    }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    to_phone = body.get("to", "").strip()
    msg_type = body.get("type", "").strip()
    body_text = body.get("body", "").strip()

    if not to_phone or not msg_type or not body_text:
        raise HTTPException(status_code=400, detail="to, type, and body are required")
    if msg_type not in ("button", "list"):
        raise HTTPException(status_code=400, detail="type must be 'button' or 'list'")

    result = await send_interactive_message(
        to_phone=to_phone,
        interactive_type=msg_type,
        body_text=body_text,
        buttons=body.get("buttons"),
        sections=body.get("sections"),
        header=body.get("header"),
        footer=body.get("footer"),
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Send failed"))


@app.post("/api/campaigns/{campaign_id}/recipients-from-contacts", dependencies=[Depends(require_auth_csrf)])
async def api_add_recipients_from_contacts(campaign_id: str, request: Request):
    """Add campaign recipients from CRM contacts filtered by stage and/or tags.

    Body: { "stage": "interested", "tags": ["ca", "mba"] }
    Both filters are optional. If neither is specified, all contacts are added.
    """
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if campaign["status"] == "running":
        raise HTTPException(status_code=400, detail="Cannot add recipients while running")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    stage = body.get("stage", "")
    tags = body.get("tags", [])

    # Fetch contacts matching criteria
    contacts = await list_contacts(limit=10000, stage=stage)

    # Filter by tags if specified
    if tags:
        tag_set = {t.lower().strip() for t in tags}
        filtered = []
        for c in contacts:
            contact_tags = c.get("tags") or []
            if isinstance(contact_tags, str):
                import json as _json
                try:
                    contact_tags = _json.loads(contact_tags)
                except Exception:
                    contact_tags = []
            if any(t.lower().strip() in tag_set for t in contact_tags):
                filtered.append(c)
        contacts = filtered

    if not contacts:
        return {"added": 0, "duplicate": 0, "invalid": 0, "message": "No matching contacts found"}

    records = [{"phone": c["phone"], "name": c.get("name", "")} for c in contacts if c.get("phone")]
    result = await add_recipients(campaign_id, records)
    return result


# --- Protected: Orders API ---


@app.get("/api/orders", dependencies=[Depends(require_auth)])
async def api_list_orders(limit: int = 100, status: str = ""):
    """List orders with optional status filter."""
    orders = await list_orders_db(limit, status)
    return {"orders": orders, "count": len(orders)}


@app.get("/api/orders/stats", dependencies=[Depends(require_auth)])
async def api_order_stats():
    """Get order count per status + total revenue."""
    return await get_order_stats()


@app.get("/api/orders/{order_id}", dependencies=[Depends(require_auth)])
async def api_get_order(order_id: str):
    """Get order detail."""
    order = await get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


# --- Razorpay Webhook (validated by signature, not session auth) ---


@app.post("/api/webhooks/razorpay")
async def razorpay_payment_webhook(request: Request):
    """Razorpay payment status webhook.

    Validates X-Razorpay-Signature header. Does not require session auth.
    """
    signature = request.headers.get("X-Razorpay-Signature", "")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    result = await handle_razorpay_webhook(payload, signature)

    if result.get("status") == "error" and result.get("reason") == "invalid signature":
        raise HTTPException(status_code=403, detail="Invalid signature")

    return result


# --- Protected: Call History API ---


@app.get("/calls", dependencies=[Depends(require_auth)])
async def list_calls(limit: int = 50):
    """List recent calls for monitoring/dashboard."""
    calls = await get_recent_calls(limit)
    return {"calls": calls, "count": len(calls)}


@app.get("/calls/{call_id}", dependencies=[Depends(require_auth)])
async def get_call_detail(call_id: str):
    """Get details for a specific call including transcript."""
    call = await get_call(call_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    return call


# --- Protected: Conversation API ---


@app.get("/api/conversations", dependencies=[Depends(require_auth)])
async def list_conversations(limit: int = 50):
    """List recent text conversations."""
    conversations = await get_recent_conversations(limit)
    return {"conversations": conversations, "count": len(conversations)}


@app.get("/api/conversations/{conversation_id}", dependencies=[Depends(require_auth)])
async def get_conversation_detail(conversation_id: str):
    """Get conversation detail with all messages."""
    conv = await get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv


# --- Protected: Resolve Handoffs ---


@app.patch("/api/calls/{call_id}/resolve", dependencies=[Depends(require_auth_csrf)])
async def resolve_call_handoff(call_id: str):
    """Mark a call handoff as resolved."""
    call = await get_call(call_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    await resolve_call(call_id)
    return {"status": "resolved", "call_id": call_id}


@app.patch("/api/conversations/{conversation_id}/resolve", dependencies=[Depends(require_auth_csrf)])
async def resolve_conversation_handoff(conversation_id: str):
    """Mark a chat handoff as resolved."""
    conv = await get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    await resolve_conversation(conversation_id)
    return {"status": "resolved", "conversation_id": conversation_id}


# --- Protected: Delete Calls ---


@app.delete("/api/calls/{call_id}", dependencies=[Depends(require_auth_csrf)])
async def delete_call_endpoint(call_id: str):
    """Delete a single call record and its recording."""
    call = await get_call(call_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")

    # Delete recording (MinIO + local)
    if call.get("recording_path"):
        if media_storage.is_configured():
            media_storage.delete_object(media_storage.build_recording_key(call_id))
        recording_path = RECORDINGS_DIR / f"{call_id}.wav"
        if recording_path.exists():
            recording_path.unlink()

    await delete_call(call_id)
    return {"success": True, "call_id": call_id}


@app.post("/api/calls/bulk-delete", dependencies=[Depends(require_auth_csrf)])
async def bulk_delete_calls(request: Request):
    """Delete multiple call records and their recordings."""
    body = await request.json()
    call_ids = body.get("ids", [])
    if not call_ids or not isinstance(call_ids, list):
        raise HTTPException(status_code=400, detail="ids array required")

    # Delete recordings for each call
    for cid in call_ids:
        if media_storage.is_configured():
            media_storage.delete_object(media_storage.build_recording_key(cid))
        recording_path = RECORDINGS_DIR / f"{cid}.wav"
        if recording_path.exists():
            recording_path.unlink()

    count = await delete_calls_bulk(call_ids)
    return {"success": True, "deleted": count}


# --- Protected: Dashboard Stats ---


@app.get("/api/stats", dependencies=[Depends(require_auth)])
async def dashboard_stats():
    """Get aggregate stats for the dashboard."""
    return await get_stats()


# --- Protected: Recordings (served through auth endpoint, not static mount) ---


@app.get("/api/recordings/{call_id}", dependencies=[Depends(require_auth)])
async def get_recording(call_id: str):
    """Stream a call recording. Tries MinIO first, falls back to local file."""
    if ".." in call_id or "/" in call_id or "\\" in call_id:
        raise HTTPException(status_code=400, detail="Invalid call ID")

    # Try MinIO presigned URL redirect
    if media_storage.is_configured():
        key = media_storage.build_recording_key(call_id)
        url = media_storage.generate_presigned_url(key)
        if url:
            return RedirectResponse(url=url)

    # Fallback to local file
    recording_path = RECORDINGS_DIR / f"{call_id}.wav"
    if not recording_path.exists():
        raise HTTPException(status_code=404, detail="Recording not found")
    return FileResponse(str(recording_path), media_type="audio/wav")


@app.delete("/api/recordings/{call_id}", dependencies=[Depends(require_auth_csrf)])
async def delete_recording(call_id: str):
    """Delete a call recording from MinIO and/or local filesystem."""
    if ".." in call_id or "/" in call_id or "\\" in call_id:
        raise HTTPException(status_code=400, detail="Invalid call ID")

    # Delete from MinIO
    if media_storage.is_configured():
        media_storage.delete_object(media_storage.build_recording_key(call_id))

    # Delete local file
    recording_path = RECORDINGS_DIR / f"{call_id}.wav"
    if recording_path.exists():
        recording_path.unlink()
        logger.info(f"Recording deleted locally: {call_id}.wav")

    # Clear recording_path in DB
    try:
        await complete_call_record(call_id, recording_path="")
    except Exception as e:
        logger.error(f"Failed to clear recording_path in DB for {call_id}: {e}")
    return {"success": True}


@app.get("/api/media/presign", dependencies=[Depends(require_auth)])
async def get_media_presigned_url(key: str = ""):
    """Generate a presigned URL for a MinIO object."""
    if not key:
        raise HTTPException(status_code=400, detail="key parameter is required")
    if ".." in key:
        raise HTTPException(status_code=400, detail="Invalid key")
    url = media_storage.generate_presigned_url(key)
    if not url:
        raise HTTPException(status_code=404, detail="Object not found or MinIO not configured")
    return {"url": url, "expires_in": media_storage.PRESIGNED_URL_EXPIRY}


# --- Protected: Knowledge API ---


@app.get("/api/knowledge", dependencies=[Depends(require_auth)])
async def list_knowledge_files():
    """List all knowledge markdown files."""
    files = []
    for md_file in sorted(KNOWLEDGE_DIR.glob("*.md")):
        stat = md_file.stat()
        files.append({
            "name": md_file.name,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        })
    return {"files": files, "count": len(files)}


@app.get("/api/knowledge/{filename}", dependencies=[Depends(require_auth)])
async def get_knowledge_file(filename: str):
    """Get content of a knowledge file."""
    filename = validate_knowledge_filename(filename)
    file_path = KNOWLEDGE_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    content = file_path.read_text(encoding="utf-8")
    return {"name": filename, "content": content}


@app.put("/api/knowledge/{filename}", dependencies=[Depends(require_auth_csrf)])
async def update_knowledge_file(filename: str, request: Request):
    """Update or create a knowledge file."""
    filename = validate_knowledge_filename(filename)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    content = body.get("content", "")
    file_path = KNOWLEDGE_DIR / filename
    file_path.write_text(content, encoding="utf-8")
    logger.info(f"Knowledge file updated: {filename} ({len(content)} chars)")
    return {"status": "saved", "name": filename}


@app.post("/api/knowledge", dependencies=[Depends(require_auth_csrf)])
async def create_knowledge_file(request: Request):
    """Create a new knowledge file."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    filename = validate_knowledge_filename(body.get("name", ""))
    content = body.get("content", "")

    file_path = KNOWLEDGE_DIR / filename
    if file_path.exists():
        raise HTTPException(status_code=409, detail="File already exists")

    file_path.write_text(content, encoding="utf-8")
    logger.info(f"Knowledge file created: {filename}")
    return {"status": "created", "name": filename}


@app.delete("/api/knowledge/{filename}", dependencies=[Depends(require_auth_csrf)])
async def delete_knowledge_file(filename: str):
    """Delete a knowledge file."""
    filename = validate_knowledge_filename(filename)
    file_path = KNOWLEDGE_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    file_path.unlink()
    logger.info(f"Knowledge file deleted: {filename}")
    return {"status": "deleted", "name": filename}


@app.post("/api/knowledge/{filename}/rename", dependencies=[Depends(require_auth_csrf)])
async def rename_knowledge_file(filename: str, request: Request):
    """Rename a knowledge file."""
    filename = validate_knowledge_filename(filename)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    new_name = validate_knowledge_filename(body.get("new_name", ""))
    old_path = KNOWLEDGE_DIR / filename
    new_path = KNOWLEDGE_DIR / new_name

    if not old_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    if new_path.exists():
        raise HTTPException(status_code=409, detail="Target filename already exists")

    old_path.rename(new_path)
    logger.info(f"Knowledge file renamed: {filename} -> {new_name}")
    return {"status": "renamed", "old_name": filename, "new_name": new_name}


# --- Dashboard ---


@app.get("/dashboard")
async def dashboard():
    """Redirect to dashboard HTML."""
    return RedirectResponse(url="/static/dashboard.html")


# --- Health (no auth) ---


@app.get("/health")
async def health():
    """Health check for Coolify."""
    return {"status": "ok", "service": "ifs-whatsapp-platform", "version": "4.0.0"}


async def run_server(host: str, port: int):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

    config = uvicorn.Config(app, host=host, port=port, log_config=None)
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    logger.info(f"IFS Voice Agent running on {host}:{port}")

    await shutdown_event.wait()

    if whatsapp_client:
        await whatsapp_client.terminate_all_calls()

    server.should_exit = True
    await server_task


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IFS WhatsApp AI Voice Agent")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    logger.remove(0)
    logger.add(sys.stderr, level="TRACE" if args.verbose else "DEBUG")

    try:
        asyncio.run(run_server(args.host, args.port))
    except KeyboardInterrupt:
        logger.info("Server stopped")
