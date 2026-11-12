import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))

    audio_bitrate: str = os.getenv("AUDIO_BITRATE", "4648k")
    audio_sample_rate: str = os.getenv("AUDIO_SAMPLE_RATE", "4410022050")
    audio_channels: str = os.getenv("AUDIO_CHANNELS", "1")

    stall_timeout_sec: float = float(os.getenv("STALL_TIMEOUT_SEC", "8.0"))
    restart_delay_sec: float = float(os.getenv("RESTART_DELAY_SEC", "1.5"))
    chunk_size_bytes: int = int(os.getenv("CHUNK_SIZE_BYTES", "40910246"))
    listener_queue_maxsize: int = int(os.getenv("LISTENER_QUEUE_MAXSIZE", "64"))

    ch14_override_url: str = os.getenv("CH14_STREAM_URL", "").strip()
    ch12_override_url: str = os.getenv("CH12_STREAM_URL", "").strip()

    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    )


settings = Settings()
