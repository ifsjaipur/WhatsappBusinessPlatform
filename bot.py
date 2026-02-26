"""IFS AI Voice Bot Pipeline

Pipecat pipeline using Gemini Live for real-time voice conversation.
Gemini handles STT + LLM + TTS natively in a single model.

Phase 2: Adds conversation transcription, call recording, handoff detection,
SQLite storage, WhatsApp follow-up messaging, and enriched n8n hooks.
Phase 3: Auto-hangup after bot says goodbye via LLM function calling.
"""

import asyncio
import datetime
import json
import os
import wave
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import EndFrame, LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport

import media_storage
from contacts_db import get_or_create_contact, is_blocked
from db import complete_call_record, create_call_record
from hooks import send_call_summary
from knowledge import load_prompt
from utils import detect_handoff, extract_topics, generate_id
from whatsapp_messaging import send_followup_message

load_dotenv(override=True)

# Ensure recordings directory exists
RECORDINGS_DIR = Path(__file__).parent / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

# Hardcoded fallback — only used if knowledge/prompt_voice.md is missing
_DEFAULT_VOICE_PROMPT = """You are an AI voice assistant for Institute of Financial Studies (IFS), Jaipur.

IMPORTANT RULES:
- You are answering a live phone call. Keep responses brief (1-3 sentences).
- Respond in the SAME LANGUAGE the caller speaks (Hindi or English).
- Only answer from the knowledge provided below. Never make up information.
- When the caller asks for details hard to convey on a call, tell them you will send it on WhatsApp after the call.
- ENDING CALLS: When the conversation is naturally ending (the caller says goodbye, thanks you, or indicates they have no more questions), say your final goodbye message and then call the end_call function. Do NOT call end_call until you have spoken your goodbye.

YOUR KNOWLEDGE:
{knowledge}
"""

# end_call function tool — lets Gemini signal when the call should end
_end_call_function = FunctionSchema(
    name="end_call",
    description=(
        "End the phone call. Call this function AFTER you have said your final goodbye "
        "message to the caller. Use when the conversation is complete, the caller says "
        "goodbye, or the caller has no more questions."
    ),
    properties={
        "reason": {
            "type": "string",
            "description": "Brief reason for ending the call, e.g. 'caller said goodbye', "
                           "'conversation complete', 'caller has no more questions'",
        },
    },
    required=["reason"],
)
_tools = ToolsSchema(standard_tools=[_end_call_function])

async def run_bot(
    webrtc_connection,
    knowledge_context: str = "",
    caller_phone: str = "",
    caller_name: str = "",
):
    """Run the AI voice bot for a single WhatsApp call.

    Captures transcript, records audio, detects handoff requests,
    stores everything in SQLite, and sends post-call notifications.
    """
    call_id = generate_id()
    logger.info(f"Call {call_id}: Starting bot for {caller_phone} ({caller_name})")

    voice_prompt = load_prompt("voice", _DEFAULT_VOICE_PROMPT)
    system_instruction = voice_prompt.format(
        knowledge=knowledge_context or "No knowledge documents loaded yet."
    )

    transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_out_10ms_chunks=2,
        ),
    )

    llm = GeminiLiveLLMService(
        api_key=os.getenv("GOOGLE_API_KEY"),
        voice_id="Kore",  # Options: Aoede, Charon, Fenrir, Kore, Puck
        system_instruction=system_instruction,
    )

    # Initial context: tell the AI to greet the caller
    context = LLMContext(
        [
            {
                "role": "user",
                "content": "A customer is calling IFS. Greet them warmly and ask how you can help.",
            }
        ],
        tools=_tools,
    )

    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(),
        ),
    )

    # Audio recording processor — captures both user and bot audio
    audiobuffer = AudioBufferProcessor(num_channels=1)

    pipeline = Pipeline(
        [
            transport.input(),
            user_aggregator,
            llm,
            transport.output(),
            audiobuffer,
            assistant_aggregator,
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    # Register end_call function — Gemini calls this to hang up after goodbye
    async def handle_end_call(params: FunctionCallParams):
        reason = params.arguments.get("reason", "conversation complete")
        logger.info(f"Call {call_id}: LLM triggered end_call — {reason}")
        await params.result_callback({"status": "ending_call", "reason": reason})
        # Wait for the goodbye audio to finish playing before ending
        await asyncio.sleep(3)
        logger.info(f"Call {call_id}: Auto-hangup executing")
        await task.queue_frame(EndFrame())

    llm.register_function("end_call", handle_end_call)

    # Call session state
    recording_filename = f"{call_id}.wav"
    recording_rel_path = f"recordings/{recording_filename}"
    call_metadata = {
        "call_id": call_id,
        "caller_phone": caller_phone,
        "caller_name": caller_name,
        "connected_at": None,
        "disconnected_at": None,
    }

    @audiobuffer.event_handler("on_audio_data")
    async def on_audio_data(buffer, audio, sample_rate, num_channels):
        """Save recorded audio to WAV file when recording stops."""
        recording_path = RECORDINGS_DIR / recording_filename
        logger.info(f"Call {call_id}: on_audio_data fired — {len(audio) if audio else 0} bytes, {sample_rate}Hz, {num_channels}ch")
        if not audio:
            logger.warning(f"Call {call_id}: Empty audio buffer, skipping save")
            return
        try:
            with wave.open(str(recording_path), "wb") as wf:
                wf.setnchannels(num_channels)
                wf.setsampwidth(2)  # 16-bit audio
                wf.setframerate(sample_rate)
                wf.writeframes(audio)
            logger.info(f"Call {call_id}: Audio saved to {recording_path} ({recording_path.stat().st_size} bytes)")

            # Upload to MinIO if configured
            if media_storage.is_configured():
                key = media_storage.build_recording_key(call_id)
                with open(str(recording_path), "rb") as f:
                    wav_bytes = f.read()
                if media_storage.upload_bytes(key, wav_bytes, "audio/wav"):
                    logger.info(f"Call {call_id}: Recording uploaded to MinIO: {key}")
                else:
                    logger.warning(f"Call {call_id}: MinIO upload failed, keeping local copy")
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to save audio: {e}")

    @transport.event_handler("on_client_connected")
    async def on_connected(transport_obj, client):
        call_metadata["connected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"Call {call_id}: Connected from {caller_phone}")

        # Block check: disconnect blocked callers immediately
        if caller_phone:
            try:
                if await is_blocked(caller_phone):
                    logger.info(f"Call {call_id}: BLOCKED caller {caller_phone}, disconnecting")
                    # Queue EndFrame for graceful pipeline shutdown (triggers on_client_disconnected)
                    await task.queue_frame(EndFrame())
                    return
            except Exception as e:
                logger.error(f"Call {call_id}: Block check failed: {e}")

        # Start audio recording
        await audiobuffer.start_recording()

        # Create initial DB record
        try:
            await create_call_record(
                call_id, caller_phone, caller_name, call_metadata["connected_at"]
            )
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to create DB record: {e}")

        # Auto-create contact in CRM for callers
        if caller_phone:
            try:
                await get_or_create_contact(caller_phone, caller_name or "")
                logger.info(f"Call {call_id}: Contact ensured for {caller_phone}")
            except Exception as e:
                logger.error(f"Call {call_id}: Failed to create contact: {e}")

        # Trigger the AI greeting
        await task.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(transport_obj, client):
        call_metadata["disconnected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"Call {call_id}: Disconnected")

        # Stop recording — triggers on_audio_data event with the full audio buffer
        await audiobuffer.stop_recording()

        # Extract transcript from LLMContext
        transcript = []
        try:
            messages = context.messages
            for msg in messages:
                role = msg.get("role", "")
                content = msg.get("content", "")
                if role in ("user", "assistant") and content:
                    transcript.append({"role": role, "content": str(content)})
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to extract transcript: {e}")

        logger.info(f"Call {call_id}: Transcript has {len(transcript)} turns")

        # Detect handoff request
        handoff_requested, handoff_reason = detect_handoff(transcript)
        if handoff_requested:
            logger.info(f"Call {call_id}: HANDOFF DETECTED — {handoff_reason}")

        # Extract topics
        topics = extract_topics(transcript)
        logger.info(f"Call {call_id}: Topics: {topics}")

        # Compute duration
        duration_seconds = 0.0
        try:
            from dateutil.parser import isoparse
            t1 = isoparse(call_metadata["connected_at"])
            t2 = isoparse(call_metadata["disconnected_at"])
            duration_seconds = (t2 - t1).total_seconds()
        except Exception:
            pass

        # Determine recording path: MinIO key or local path
        if media_storage.is_configured():
            recording_store_path = media_storage.build_recording_key(call_id)
        else:
            recording_store_path = recording_rel_path

        # Update database
        try:
            await complete_call_record(
                call_id,
                disconnected_at=call_metadata["disconnected_at"],
                duration_seconds=duration_seconds,
                transcript=json.dumps(transcript, ensure_ascii=False),
                recording_path=recording_store_path,
                handoff_requested=1 if handoff_requested else 0,
                handoff_reason=handoff_reason,
                topics=json.dumps(topics, ensure_ascii=False),
                status="handoff_pending" if handoff_requested else "completed",
            )
            logger.info(f"Call {call_id}: DB record updated")
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to update DB: {e}")

        # Send personalized WhatsApp follow-up text message
        if caller_phone:
            try:
                await send_followup_message(
                    caller_phone,
                    caller_name,
                    handoff_requested,
                    transcript=transcript,
                    topics=topics,
                    knowledge_context=knowledge_context,
                )
            except Exception as e:
                logger.error(f"Call {call_id}: Failed to send WhatsApp text: {e}")

        # Send enriched data to n8n
        try:
            await send_call_summary({
                "call_id": call_id,
                "caller_phone": caller_phone,
                "caller_name": caller_name,
                "connected_at": call_metadata["connected_at"],
                "disconnected_at": call_metadata["disconnected_at"],
                "duration_seconds": duration_seconds,
                "transcript": transcript,
                "handoff_requested": handoff_requested,
                "handoff_reason": handoff_reason,
                "topics": topics,
                "recording_path": recording_rel_path,
            })
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to send to n8n: {e}")

        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)
