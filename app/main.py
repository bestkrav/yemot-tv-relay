import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from app.stream_manager import StreamManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

manager = StreamManager()


@asynccontextmanager
async def lifespan(_: FastAPI):
    manager.start_all()
    try:
        yield
    finally:
        await manager.stop_all()


app = FastAPI(title="Yemot TV Audio Relay", version="1.0.0", lifespan=lifespan)

STREAM_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
    "Connection": "keep-alive",
    "icy-name": "Yemot TV Live Relay",
}


@app.get("/stream/ch14.mp3")
async def stream_ch14():
    worker = manager.get("ch14")
    return StreamingResponse(
        worker.subscribe(),
        media_type="audio/mpeg",
        headers=STREAM_HEADERS,
    )


@app.get("/stream/ch12.mp3")
async def stream_ch12():
    worker = manager.get("ch12")
    return StreamingResponse(
        worker.subscribe(),
        media_type="audio/mpeg",
        headers=STREAM_HEADERS,
    )


@app.get("/health")
async def health():
    channels_health = {cid: w.health() for cid, w in manager.channels.items()}
    any_running = any(c["status"] == "running" for c in channels_health.values())
    if not any_running:
        raise HTTPException(status_code=503, detail=channels_health)
    return {"ok": True, "channels": channels_health}
