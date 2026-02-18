"""IFS WhatsApp AI Voice Agent Server

FastAPI server that auto-accepts incoming WhatsApp calls and connects them
to a Gemini Live AI agent that answers queries using IFS knowledge docs.

Receives call webhooks forwarded from n8n, handles WebRTC via aiortc,
and posts call summaries back to n8n for automation.
"""

import argparse
import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from loguru import logger
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.whatsapp.api import WhatsAppWebhookRequest
from pipecat.transports.whatsapp.client import WhatsAppClient

from bot import run_bot
from knowledge import load_knowledge

load_dotenv(override=True)
import os

# Config
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_APP_SECRET = os.getenv("WHATSAPP_APP_SECRET", "")
WHATSAPP_WEBHOOK_VERIFICATION_TOKEN = os.getenv("WHATSAPP_WEBHOOK_VERIFICATION_TOKEN", "ifs_verify")
PORT = int(os.getenv("PORT", "7860"))

if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
    missing = [v for v, val in [
        ("WHATSAPP_TOKEN", WHATSAPP_TOKEN),
        ("WHATSAPP_PHONE_NUMBER_ID", WHATSAPP_PHONE_NUMBER_ID),
    ] if not val]
    raise ValueError(f"Missing required env vars: {', '.join(missing)}")

# Global state
whatsapp_client: Optional[WhatsAppClient] = None
shutdown_event = asyncio.Event()
knowledge_context = ""


def signal_handler():
    logger.info("Shutdown signal received")
    shutdown_event.set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global whatsapp_client, knowledge_context

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


app = FastAPI(title="IFS WhatsApp AI Voice Agent", version="1.0.0", lifespan=lifespan)


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

    # Reload knowledge on each call (allows updating docs without restart)
    current_knowledge = load_knowledge()

    async def connection_callback(connection: SmallWebRTCConnection):
        try:
            logger.info(f"Auto-accepted call, starting AI bot: {connection.pc_id}")
            background_tasks.add_task(run_bot, connection, current_knowledge)
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


@app.get("/health")
async def health():
    """Health check for Coolify."""
    return {"status": "ok", "service": "ifs-voice-agent"}


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
