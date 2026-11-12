import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Optional

import httpx

from app.config import settings

logger = logging.getLogger("relay.extractors")

M3U8_REGEX = re.compile(r'https?://[^\s"<>\\]+\.m3u8[^\s"<>\\]*')


@dataclass
class ResolvedStream:
    url: str
    referer: str
    user_agent: str = settings.user_agent


async def _try_streamlink(page_url: str, referer: str) -> Optional[str]:
    cmd = [
        "streamlink",
        "--stream-url",
        "--http-header",
        f"User-Agent={settings.user_agent}",
        "--http-header",
        f"Referer={referer}",
        page_url,
        "worst,best",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15.0)
        if proc.returncode == 0:
            lines = stdout.decode("utf-8", errors="ignore").strip().splitlines()
            if lines and lines[-1].startswith("http"):
                logger.info("Streamlink resolved URL for %s", page_url)
                return lines[-1]
        logger.debug(
            "Streamlink did not resolve %s (rc=%s): %s",
            page_url,
            proc.returncode,
            stderr.decode("utf-8", errors="ignore").strip(),
        )
    except Exception as exc:
        logger.warning("Streamlink error for %s: %s", page_url, exc)
    return None


async def resolve_ch14() -> ResolvedStream:
    referer = "https://www.now14.co.il/"
    if settings.ch14_override_url:
        return ResolvedStream(url=settings.ch14_override_url, referer=referer)

    candidate_pages = [
        "https://www.now14.co.il/tochniot_meleot/%D7%A9%D7%99%D7%93%D7%95%D7%A8-%D7%97%D7%99/",
        "https://www.now14.co.il/",
        "https://www.c14.co.il/live",
    ]

    for page_url in candidate_pages:
        sl_url = await _try_streamlink(page_url, referer)
        if sl_url:
            return ResolvedStream(url=sl_url, referer=referer)

    headers = {
        "User-Agent": settings.user_agent,
        "Referer": referer,
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    }
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        for page_url in candidate_pages:
            try:
                resp = await client.get(page_url, headers=headers)
                if resp.status_code == 200:
                    matches = M3U8_REGEX.findall(resp.text)
                    if matches:
                        logger.info("HTML extractor resolved CH14 from %s", page_url)
                        return ResolvedStream(url=matches[0], referer=referer)
            except Exception as exc:
                logger.warning("CH14 fetch error on %s: %s", page_url, exc)

    raise RuntimeError("Unable to resolve an active live stream URL for Channel 14")


async def resolve_ch12() -> ResolvedStream:
    referer = "https://www.mako.co.il/"
    if settings.ch12_override_url:
        return ResolvedStream(url=settings.ch12_override_url, referer=referer)

    candidate_pages = [
        "https://www.mako.co.il/tv-live",
        "https://www.12plus.co.il/",
    ]
    for page_url in candidate_pages:
        sl_url = await _try_streamlink(page_url, referer)
        if sl_url:
            return ResolvedStream(url=sl_url, referer=referer)

    headers = {
        "User-Agent": settings.user_agent,
        "Referer": referer,
    }
    n12_playlist_url = (
        "https://www.n12.co.il/AjaxPage?jspName=playlist.jsp"
        "&vcmid=1e2258089b67f510VgnVCM2000002a0c10acRCRD"
        "&videoChannelId=31750a2610f26110VgnVCM1000005201000aRCRD"
        "&galleryChannelId=1e2258089b67f510VgnVCM2000002a0c10acRCRD"
        "&isGallery=false&consumer=web_html5&encryption=no"
    )
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        try:
            resp = await client.get(n12_playlist_url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                media_list = data.get("media", [])
                for item in media_list:
                    m_url = item.get("url", "")
                    if ".m3u8" in m_url:
                        entitlement_url = (
                            "https://mass.mako.co.il/ClicksStatistics/"
                            f"entitlementsServicesV2.jsp?et=gt&lp={m_url}&rv=AKAMAI"
                        )
                        ent_resp = await client.get(entitlement_url, headers=headers)
                        if ent_resp.status_code == 200:
                            ent_data = ent_resp.json()
                            tickets = ent_data.get("tickets", [])
                            if tickets and tickets[0].get("url"):
                                ticket_url = tickets[0]["url"]
                                ticket = tickets[0].get("ticket", "")
                                sep = "&" if "?" in ticket_url else "?"
                                final_url = f"{ticket_url}{sep}{ticket}" if ticket else ticket_url
                                logger.info("Mako entitlements resolved CH12 URL")
                                return ResolvedStream(url=final_url, referer=referer)
                        return ResolvedStream(url=m_url, referer=referer)
        except Exception as exc:
            logger.warning("CH12 Mako/N12 API error: %s", exc)

        for page_url in candidate_pages:
            try:
                resp = await client.get(page_url, headers=headers)
                if resp.status_code == 200:
                    matches = M3U8_REGEX.findall(resp.text)
                    if matches:
                        logger.info("HTML extractor resolved CH12 from %s", page_url)
                        return ResolvedStream(url=matches[0], referer=referer)
            except Exception as exc:
                logger.warning("CH12 fetch error on %s: %s", page_url, exc)

    raise RuntimeError("Unable to resolve an active live stream URL for Channel 12")
