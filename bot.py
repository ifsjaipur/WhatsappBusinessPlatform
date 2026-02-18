"""IFS AI Voice Bot Pipeline

Pipecat pipeline using Gemini Live for real-time voice conversation.
Gemini handles STT + LLM + TTS natively in a single model.
"""

import os

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
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport

from hooks import send_call_summary

load_dotenv(override=True)

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


async def run_bot(webrtc_connection, knowledge_context: str = ""):
    """Run the AI voice bot for a single WhatsApp call."""

    system_instruction = AGENT_RULES.format(knowledge=knowledge_context or "No knowledge documents loaded yet.")

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

    pipeline = Pipeline(
        [
            transport.input(),
            user_aggregator,
            llm,
            transport.output(),
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

    # Track call metadata for post-call hook
    call_metadata = {
        "connected_at": None,
        "disconnected_at": None,
    }

    @transport.event_handler("on_client_connected")
    async def on_connected(transport, client):
        import datetime
        call_metadata["connected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info("Call connected - AI greeting caller")
        await task.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(transport, client):
        import datetime
        call_metadata["disconnected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info("Call disconnected")
        await task.cancel()

        # Send call summary to n8n
        try:
            await send_call_summary(call_metadata)
        except Exception as e:
            logger.error(f"Failed to send call summary: {e}")

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)
