"""IFS AI Voice Bot Pipeline

Pipecat pipeline using Gemini Live for real-time voice conversation.
Gemini handles STT + LLM + TTS natively in a single model.

Phase 2: Adds conversation transcription, call recording, handoff detection,
SQLite storage, WhatsApp follow-up messaging, and enriched n8n hooks.
"""

import datetime
import json
import os
import wave
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
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
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport

from db import complete_call_record, create_call_record
from hooks import send_call_summary
from utils import detect_handoff, extract_topics, generate_id
from whatsapp_messaging import send_followup_message

load_dotenv(override=True)

# Ensure recordings directory exists
RECORDINGS_DIR = Path(__file__).parent / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

AGENT_RULES = """You are an AI voice assistant for Institute of Financial Studies (IFS), Jaipur.

IMPORTANT RULES:
- You are answering a live phone call. Keep responses brief (1-3 sentences).
- Speak naturally like a helpful receptionist, not like a chatbot.
- Respond in the SAME LANGUAGE the caller speaks (Hindi or English).
- Only answer from the knowledge provided below. Never make up information.
- If you don't know something, say: "I would be happy to connect you with our admissions team for more details. You can also reach us at our office number."
- Do not use special characters, emojis, or markdown in your responses (output is converted to speech).
- Be warm, professional, and helpful.
- If the caller wants to speak to a human, say: "I will let our team know. Someone will call you back shortly."

YOUR KNOWLEDGE:
{knowledge}
"""

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

    system_instruction = AGENT_RULES.format(
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
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to save audio: {e}")

    @transport.event_handler("on_client_connected")
    async def on_connected(transport_obj, client):
        call_metadata["connected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"Call {call_id}: Connected from {caller_phone}")

        # Start audio recording
        await audiobuffer.start_recording()

        # Create initial DB record
        try:
            await create_call_record(
                call_id, caller_phone, caller_name, call_metadata["connected_at"]
            )
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to create DB record: {e}")

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

        # Update database
        try:
            await complete_call_record(
                call_id,
                disconnected_at=call_metadata["disconnected_at"],
                duration_seconds=duration_seconds,
                transcript=json.dumps(transcript, ensure_ascii=False),
                recording_path=recording_rel_path,
                handoff_requested=1 if handoff_requested else 0,
                handoff_reason=handoff_reason,
                topics=json.dumps(topics, ensure_ascii=False),
                status="handoff_pending" if handoff_requested else "completed",
            )
            logger.info(f"Call {call_id}: DB record updated")
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to update DB: {e}")

        # Send WhatsApp follow-up text message
        if caller_phone:
            try:
                await send_followup_message(caller_phone, caller_name, handoff_requested)
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
