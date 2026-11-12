import asyncio
import contextlib
import logging
import time
from collections import deque
from typing import AsyncIterator, Awaitable, Callable, Deque, Dict, Set

from app.config import settings
from app.extractors import ResolvedStream, resolve_ch12, resolve_ch14

logger = logging.getLogger("relay.stream_manager")


class ChannelBroadcaster:
    def __init__(self, channel_id: str, resolver: Callable[[], Awaitable[ResolvedStream]]):
        self.channel_id = channel_id
        self.resolver = resolver
        self._listeners: Set[asyncio.Queue[bytes]] = set()
        self._recent_chunks: Deque[bytes] = deque(maxlen=6)
        self._task: asyncio.Task | None = None
        self._process: asyncio.subprocess.Process | None = None
        self._stop_event = asyncio.Event()

        self.status: str = "initializing"
        self.last_chunk_ts: float = 0.0
        self.restarts: int = 0
        self.last_error: str = ""

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._watchdog_loop(), name=f"worker-{self.channel_id}")

    async def stop(self) -> None:
        self._stop_event.set()
        await self._terminate_process()
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self.status = "stopped"

    async def _terminate_process(self) -> None:
        proc = self._process
        self._process = None
        if proc and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await proc.wait()

    def _broadcast(self, chunk: bytes) -> None:
        self._recent_chunks.append(chunk)
        for q in list(self._listeners):
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(chunk)

    async def _run_pipeline_once(self) -> None:
        resolved = await self.resolver()
        headers_str = (
            f"User-Agent: {resolved.user_agent}\r\n"
            f"Referer: {resolved.referer}\r\n"
        )

        ffmpeg_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-fflags", "+nobuffer+discardcorrupt",
            "-flags", "low_delay",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "2",
            "-headers", headers_str,
            "-i", resolved.url,
            "-vn",
            "-acodec", "libmp3lame",
            "-ac", settings.audio_channels,
            "-ab", settings.audio_bitrate,
            "-ar", settings.audio_sample_rate,
            "-f", "mp3",
            "-flush_packets", "1",
            "pipe:1",
        ]

        logger.info("[%s] Starting FFmpeg pipeline...", self.channel_id)
        self._process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.status = "running"
        self.last_error = ""

        assert self._process.stdout is not None
        assert self._process.stderr is not None

        async def _drain_stderr() -> None:
            assert self._process and self._process.stderr
            while not self._stop_event.is_set():
                line = await self._process.stderr.readline()
                if not line:
                    break
                logger.warning("[%s][ffmpeg] %s", self.channel_id, line.decode("utf-8", errors="ignore").strip())

        stderr_task = asyncio.create_task(_drain_stderr())

        try:
            while not self._stop_event.is_set():
                try:
                    chunk = await asyncio.wait_for(
                        self._process.stdout.read(settings.chunk_size_bytes),
                        timeout=settings.stall_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "[%s] Watchdog: No audio data for %.1fs. Restarting...",
                        self.channel_id,
                        settings.stall_timeout_sec,
                    )
                    self.last_error = "stream_stalled"
                    break

                if not chunk:
                    logger.warning("[%s] FFmpeg EOF reached.", self.channel_id)
                    self.last_error = "ffmpeg_eof"
                    break

                self.last_chunk_ts = time.time()
                self._broadcast(chunk)
        finally:
            stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stderr_task
            await self._terminate_process()

    async def _watchdog_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._run_pipeline_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                logger.error("[%s] Pipeline error: %s", self.channel_id, exc)

            if self._stop_event.is_set():
                break

            self.status = "restarting"
            self.restarts += 1
            logger.info(
                "[%s] Reconnecting in %.1fs (restart #%d)...",
                self.channel_id,
                settings.restart_delay_sec,
                self.restarts,
            )
            await asyncio.sleep(settings.restart_delay_sec)

    async def subscribe(self) -> AsyncIterator[bytes]:
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=settings.listener_queue_maxsize)
        self._listeners.add(queue)
        logger.info("[%s] Listener connected (total=%d)", self.channel_id, len(self._listeners))
        try:
            for buffered in list(self._recent_chunks):
                yield buffered
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            self._listeners.discard(queue)
            logger.info("[%s] Listener disconnected (total=%d)", self.channel_id, len(self._listeners))

    def health(self) -> dict:
        age = round(time.time() - self.last_chunk_ts, 2) if self.last_chunk_ts else None
        return {
            "channel": self.channel_id,
            "status": self.status,
            "listeners": len(self._listeners),
            "restarts": self.restarts,
            "seconds_since_last_audio": age,
            "last_error": self.last_error or None,
        }


class StreamManager:
    def __init__(self) -> None:
        self.channels: Dict[str, ChannelBroadcaster] = {
            "ch14": ChannelBroadcaster("ch14", resolve_ch14),
            "ch12": ChannelBroadcaster("ch12", resolve_ch12),
        }

    def start_all(self) -> None:
        for worker in self.channels.values():
            worker.start()

    async def stop_all(self) -> None:
        await asyncio.gather(*(w.stop() for w in self.channels.values()))

    def get(self, channel_id: str) -> ChannelBroadcaster:
        return self.channels[channel_id]
