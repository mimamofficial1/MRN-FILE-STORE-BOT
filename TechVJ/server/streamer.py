"""
Fast Download / Watch Online streaming support.

This is a single-file, single-bot-client adaptation of the well known
Telegram "file-to-link" streaming technique (the same idea used by
DreamXBotz / FileStreamBot and friends): instead of re-downloading a
file to disk, Telegram's raw upload.GetFile API is called directly and
the bytes are streamed straight to the browser, chunk by chunk, with
proper HTTP Range support so a <video> tag can seek/play instantly and
a download manager can resume a partial download.

Design notes specific to this project (kept intentionally simpler than
a lot of the reference bots this technique is copied from):

- Single bot client only (no MULTI_CLIENT / work_loads load-balancing).
  This project runs one bot token, so there is nothing to balance.
- No separate BIN_CHANNEL / forwarding step. Every file this bot ever
  hands out is fetched by (chat_id, message_id) - that's exactly the
  same (channel, message id) pair genlink.py/commands.py already read
  the file from - so the stream link is built from data already in
  hand, with zero extra Telegram API calls.
- The (chat_id, message_id) pair is packed into one URL-safe token
  instead of a raw numeric path, since chat_id for a channel is a
  large negative number.
"""

import base64
import hashlib
import hmac
import html
import logging
import math
import mimetypes
import re
import time
import urllib.parse
from typing import Any, Dict, Optional, Tuple

from aiohttp import web
from pyrogram import raw, utils
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session

from config import BOT_TOKEN, STREAM_LINK_TTL, URL

logger = logging.getLogger(__name__)

MEDIA_TYPES = ("document", "video", "audio", "photo", "animation", "voice", "video_note")
CHUNK_SIZE = 1024 * 1024  # 1 MiB, must stay a multiple of 4096 (Telegram requirement)


class StreamFileNotFound(Exception):
    message = "File not found. The link may be broken or the file was removed."


class InvalidStreamHash(Exception):
    message = "Invalid or expired link."


# ---------------------------------------------------------------------------
# Token = base64("<chat_id>:<message_id>:<issued_at>"), URL-safe, no padding.
# The "hash" query param is an HMAC-SHA256 signature (keyed with the bot's own
# token, which an attacker can't know) over that same triple, truncated to 16
# hex chars (64 bits) - far stronger than slicing a few characters off the
# file's own file_unique_id, which (a) has much lower real entropy than its
# length suggests, since Telegram's unique-id encoding front-loads structural
# bytes, and (b) could only be checked AFTER already calling Telegram's API to
# fetch the message, making every guess cost nothing to try locally once that
# message was ever cached. Signing the token itself means the signature can be
# verified BEFORE touching Telegram at all, and the embedded timestamp lets a
# link expire (STREAM_LINK_TTL, 0 = never).
# ---------------------------------------------------------------------------

def encode_stream_token(chat_id: int, message_id: int, issued_at: Optional[int] = None) -> str:
    issued_at = int(time.time()) if issued_at is None else issued_at
    raw_token = f"{chat_id}:{message_id}:{issued_at}".encode("ascii")
    return base64.urlsafe_b64encode(raw_token).decode("ascii").rstrip("=")


def decode_stream_token(token: str) -> Tuple[int, int, int]:
    padded = token + "=" * (-len(token) % 4)
    chat_id_str, message_id_str, issued_at_str = base64.urlsafe_b64decode(padded.encode("ascii")).decode("ascii").split(":", 2)
    return int(chat_id_str), int(message_id_str), int(issued_at_str)


def _compute_signature(chat_id: int, message_id: int, issued_at: int) -> str:
    msg = f"{chat_id}:{message_id}:{issued_at}".encode("ascii")
    key = (BOT_TOKEN or "insecure-fallback-key").encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()[:16]


def get_media_from_message(message) -> Any:
    for attr in MEDIA_TYPES:
        media = getattr(message, attr, None)
        if media:
            return media
    return None


def resolve_stream_token(token: str, provided_sig: str) -> Tuple[int, int]:
    """Decode a /dl/ or /watch/ token and verify its signature + expiry
    BEFORE any Telegram API call is made. Raises InvalidStreamHash if the
    token is malformed, the signature doesn't match, or the link expired."""
    try:
        chat_id, message_id, issued_at = decode_stream_token(token)
    except Exception:
        raise InvalidStreamHash
    expected = _compute_signature(chat_id, message_id, issued_at)
    if not hmac.compare_digest(expected, provided_sig or ""):
        raise InvalidStreamHash
    if STREAM_LINK_TTL and (int(time.time()) - issued_at) > STREAM_LINK_TTL:
        raise InvalidStreamHash
    return chat_id, message_id


def _dl_url(chat_id: int, message_id: int, file_name: str, force_download: bool, issued_at: int) -> str:
    token = encode_stream_token(chat_id, message_id, issued_at)
    sig = _compute_signature(chat_id, message_id, issued_at)
    safe_name = urllib.parse.quote(file_name or "file")
    base = URL if URL.endswith("/") else URL + "/"
    query = f"hash={sig}"
    if force_download:
        query += "&dl=1"  # tells stream_media() to send Content-Disposition: attachment
    return f"{base}dl/{token}/{safe_name}?{query}"


def build_stream_urls(chat_id: int, message_id: int, file_name: str) -> Tuple[str, str]:
    """Build (download_url, watch_url) for a file already sitting at
    (chat_id, message_id) - no network call, pure string building.
    download_url forces an actual download (dl=1); watch_url opens the
    HTML player page, whose <video>/<audio> src is a separate, inline
    (non-forced) link so it plays instead of downloading."""
    issued_at = int(time.time())
    download_url = _dl_url(chat_id, message_id, file_name, force_download=True, issued_at=issued_at)
    token = encode_stream_token(chat_id, message_id, issued_at)
    sig = _compute_signature(chat_id, message_id, issued_at)
    base = URL if URL.endswith("/") else URL + "/"
    watch_url = f"{base}watch/{token}?hash={sig}"
    return download_url, watch_url


def humanbytes(size) -> str:
    if not size:
        return "0 B"
    power = 1024
    n = 0
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(size)
    while size >= power and n < len(units) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {units[n]}"


class ByteStreamer:
    """Holds cached FileId properties for a single Pyrogram client and
    yields raw media bytes straight from Telegram's DC servers."""

    def __init__(self, client):
        self.client = client
        self.cached_file_ids: Dict[Tuple[int, int], FileId] = {}

    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        key = (chat_id, message_id)
        cached = self.cached_file_ids.get(key)
        if cached is not None:
            return cached
        return await self._generate_file_properties(chat_id, message_id)

    async def _generate_file_properties(self, chat_id: int, message_id: int) -> FileId:
        message = await self.client.get_messages(chat_id, message_id)
        if not message or message.empty:
            raise StreamFileNotFound
        media = get_media_from_message(message)
        if not media:
            raise StreamFileNotFound
        file_id = FileId.decode(media.file_id)
        file_id.file_size = getattr(media, "file_size", 0) or 0
        file_id.mime_type = getattr(media, "mime_type", "") or ""
        file_id.file_name = getattr(media, "file_name", "") or ""
        file_id.unique_id = getattr(media, "file_unique_id", "") or ""
        self.cached_file_ids[(chat_id, message_id)] = file_id
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        client = self.client
        media_session = client.media_sessions.get(file_id.dc_id, None)
        if media_session is not None:
            return media_session

        if file_id.dc_id != await client.storage.dc_id():
            media_session = Session(
                client, file_id.dc_id,
                await Auth(client, file_id.dc_id, await client.storage.test_mode()).create(),
                await client.storage.test_mode(), is_media=True,
            )
            await media_session.start()
            for _ in range(6):
                exported_auth = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id))
                try:
                    await media_session.send(
                        raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes)
                    )
                    break
                except AuthBytesInvalid:
                    continue
            else:
                await media_session.stop()
                raise AuthBytesInvalid
        else:
            media_session = Session(
                client, file_id.dc_id, await client.storage.auth_key(),
                await client.storage.test_mode(), is_media=True,
            )
            await media_session.start()

        client.media_sessions[file_id.dc_id] = media_session
        return media_session

    @staticmethod
    async def get_location(file_id: FileId):
        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    async def yield_file(self, file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size):
        media_session = await self.generate_media_session(file_id)
        location = await self.get_location(file_id)
        current_part = 1
        try:
            r = await media_session.send(raw.functions.upload.GetFile(location=location, offset=offset, limit=chunk_size))
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        break
                    elif part_count == 1:
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        yield chunk[first_part_cut:]
                    elif current_part == part_count:
                        yield chunk[:last_part_cut]
                    else:
                        yield chunk
                    current_part += 1
                    offset += chunk_size
                    if current_part > part_count:
                        break
                    r = await media_session.send(raw.functions.upload.GetFile(location=location, offset=offset, limit=chunk_size))
        except (TimeoutError, AttributeError):
            pass


# Created lazily (on first request) rather than at import time, since building
# it needs a running asyncio loop and this module is imported before the bot
# actually starts polling/idling.
_streamer: Optional[ByteStreamer] = None


def get_streamer() -> ByteStreamer:
    global _streamer
    if _streamer is None:
        from TechVJ.bot import StreamBot  # local import: avoids any import-order surprises
        _streamer = ByteStreamer(StreamBot)
    return _streamer


async def stream_media(request: web.Request, chat_id: int, message_id: int) -> web.Response:
    # NOTE: the link's signature + expiry must already be verified by the
    # caller (via resolve_stream_token) before this is ever called - no
    # authorization check happens here, only the actual byte streaming.
    streamer = get_streamer()
    file_id = await streamer.get_file_properties(chat_id, message_id)

    file_size = file_id.file_size
    range_header = request.headers.get("Range")
    if range_header:
        from_bytes, _, until_bytes = range_header.replace("bytes=", "").partition("-")
        from_bytes = int(from_bytes) if from_bytes else 0
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    if (until_bytes >= file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416, body=b"416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    until_bytes = min(until_bytes, file_size - 1)
    offset = from_bytes - (from_bytes % CHUNK_SIZE)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % CHUNK_SIZE + 1
    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / CHUNK_SIZE) - math.floor(offset / CHUNK_SIZE)

    body = streamer.yield_file(file_id, offset, first_part_cut, last_part_cut, part_count, CHUNK_SIZE)

    mime_type = file_id.mime_type or (mimetypes.guess_type(file_id.file_name or "")[0]) or "application/octet-stream"
    file_name = file_id.file_name or "file"
    disposition = "attachment" if request.rel_url.query.get("dl") == "1" else "inline"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        },
    )


_WATCH_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<meta name="color-scheme" content="dark light">

<title>__FILE_NAME__ | MRN Officialx</title>

<link rel="icon" href="https://files.catbox.moe/idl5a5.png" type="image/jpeg">

<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">

<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --gold: #7c3aed;
    --gold-light: #a78bfa;
    --pink: #06b6d4;
    --dark: #14101f;
    --card-bg: #f5f0ff;
    --text: #14101f;
    --muted: #6b6178;
    --cream: #faf8ff;
  }

  body {
    font-family: 'Poppins', sans-serif;
    background: var(--bg);
    overflow-x: hidden;
    color: var(--text);
    min-height: 100vh;
    transition: background 0.3s ease, color 0.3s ease;
  }

  /* ── HEADER ── */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 20px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    box-shadow: 0 2px 12px rgba(0,0,0,0.07);
    position: sticky;
    top: 0;
    z-index: 100;
  }

  .logo-wrap { display: flex; align-items: center; gap: 10px; }
  .logo-icon { width: 42px; height: 42px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 24px; text-decoration: none; overflow: hidden; }

  .icon-row { width: 110px; height: 110px; border-radius: 50%; object-fit: cover; margin-bottom: 25px; animation: floatLogo 4s ease-in-out infinite; }

  .logo-icon img { width: 100%; height: 100%; object-fit: cover; border-radius: 50%; }
  .icon-row img { width: 100%; height: 100%; object-fit: cover; border-radius: 50%; }

  .logo-text {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 26px;
    letter-spacing: 2px;
    background: linear-gradient(90deg, var(--gold), var(--pink));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    text-decoration: none;
  }

  .header-center {
    font-family: 'Poppins', sans-serif;
    font-size: 14px;
    font-weight: 700;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #fff;
    padding: 10px 22px;
    background: linear-gradient(90deg, #7c3aed, #06b6d4);
    border-radius: 50px;
  }

  .join-btn-header {
    background: linear-gradient(135deg, var(--gold), #5b21b6);
    color: #fff;
    border: none;
    padding: 10px 20px;
    border-radius: 50px;
    font-family: 'Poppins', sans-serif;
    font-weight: 600;
    font-size: 14px;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 7px;
    text-decoration: none;
    box-shadow: 0 2px 5px rgba(124, 58, 237, 0.35);
    transition: all .3s ease;
  }
  .join-btn-header:hover { transform: translateY(-2px); box-shadow: 0 5px 10px rgba(6, 182, 212, 0.45); }

  /* ── HERO ── */
  .hero { text-align: center; padding: 90px 5% 60px; max-width: 900px; margin: 0 auto; display: flex; flex-direction: column; align-items: center; }

  .badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: var(--surface);
    border: 1.5px solid var(--gold);
    color: var(--gold);
    font-size: 12px;
    font-weight: 600;
    padding: 6px 16px;
    border-radius: 50px;
    margin-bottom: 20px;
    letter-spacing: 0.5px;
  }

  .badge-dot { width: 7px; height: 7px; background: var(--gold); border-radius: 50%; animation: pulse 1.8s ease-in-out infinite; }
  @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(0.7); } }

  .hero-title { font-family: 'Bebas Neue', sans-serif; font-size: clamp(50px, 7vw, 78px); letter-spacing: 2px; line-height: 1.2; color: var(--text); margin-bottom: 15px; }

  .hero-subtitle {
    font-family: 'Bebas Neue', sans-serif;
    font-size: clamp(50px, 7vw, 78px);
    letter-spacing: 2px;
    background: linear-gradient(90deg, #7c3aed, #06b6d4);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 30px;
    line-height: 0.8;
  }

  .hero-desc { font-size: 18px; color: var(--muted); line-height: 1.7; max-width: 650px; margin: 0 auto 40px; }

  .join-btn-hero {
    display: inline-flex; align-items: center; gap: 8px;
    background: linear-gradient(135deg, var(--gold), #5b21b6); color: #fff; text-decoration: none;
    font-weight: 700; font-size: 16px; padding: 15px 36px; border-radius: 50px;
    box-shadow: 0 6px 20px rgba(124, 58, 237, 0.35); transition: all .3s ease;
  }
  .join-btn-hero:hover { transform: translateY(-2px); box-shadow: 0 10px 28px rgba(6, 182, 212, 0.45); }

  /* ── STATS ── */
  .stats-strip { display: flex; justify-content: center; gap: 0; flex-wrap: wrap; max-width: 700px; margin: 10px auto 20px; padding: 0 6%; }
  .stat { flex: 1; min-width: 140px; text-align: center; padding: 16px 12px; border-left: 1px solid var(--border); }
  .stat:first-child { border-left: none; }
  .stat-num { font-family: 'Bebas Neue', sans-serif; font-size: 28px; letter-spacing: 1px; background: linear-gradient(90deg, var(--gold), var(--pink)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
  .stat-label { font-size: 12px; color: var(--muted); margin-top: 2px; }

  /* ── GENRE TAGS ── */
  .genres { display: flex; justify-content: center; gap: 10px; flex-wrap: wrap; padding: 22px 20px 10px; }
  .genre-tag { background: var(--surface); border: 1.5px solid var(--gold); color: var(--gold); font-size: 13px; font-weight: 600; padding: 7px 18px; border-radius: 50px; transition: all .3s ease; }
  .genre-tag:hover { transform: scale(1.05); }

  /* ── ABOUT ── */
  .section-label { text-align: center; margin: 60px 0 20px; }
  .section-label span {
    display: inline-flex; background: transparent; border: 1px solid var(--gold); color: var(--gold);
    font-size: 12px; letter-spacing: 2px; font-weight: 600; padding: 8px 20px; border-radius: 50px;
    text-transform: uppercase; gap: 8px;
  }

  .about { padding: 0 20px 60px; max-width: 900px; margin: 0 auto; text-align: center; }
  .about h2 { font-family: 'Bebas Neue', sans-serif; font-size: 36px; font-weight: 700; letter-spacing: 1.5px; color: var(--text); margin-bottom: 12px; text-align: center; }
  .about p { font-size: 16px; color: var(--muted); line-height: 1.7; max-width: 650px; text-align: center; margin: 0 auto; }

  /* ── PLAN CARDS — side by side, not stacked ── */
  .plans { padding: 8px 16px 40px; display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 20px; max-width: 740px; margin: 0 auto; }

  .plan-card {
    background: var(--surface); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px);
    border-radius: 18px; padding: 24px 20px; border: 1px solid var(--border);
    box-shadow: 0 4px 18px rgba(124,58,237,.08); transition: all .35s ease; position: relative; overflow: hidden;
  }
  .plan-card:hover { border-color: var(--accent); transform: translateY(-6px); box-shadow: 0 10px 30px rgba(124,58,237,0.2); }
  .plan-card:hover .plan-icon { transform: scale(1.08); box-shadow: 0 0 25px rgba(124,58,237,.35), 0 0 60px rgba(124,58,237,.15); }

  .plan-header { display: flex; align-items: center; gap: 12px; margin-bottom: 14px; }

  .plan-icon {
    width: 68px; height: 68px; border-radius: 22px; display: flex; align-items: center; justify-content: center;
    font-size: 28px; border: 1px solid rgba(124,58,237,.18);
    box-shadow: 0 0 25px rgba(124,58,237,.18), inset 0 0 15px rgba(124,58,237,.35);
    transition: all .4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
  }
  .plan-card:hover .plan-icon { background: linear-gradient(135deg, #7c3aed, #06b6d4); transform: rotate(-8deg) scale(1.08); }
  .plan-icon.free-icon { background: rgba(6,182,212,.15); border: 1px solid rgba(6,182,212,.25); }
  .plan-icon.prem-icon { background: rgba(124,58,237,.15); border: 1px solid rgba(124,58,237,.25); }

  .plan-title { font-weight: 700; font-size: 16px; color: var(--text); }
  .plan-tag { font-size: 11px; font-weight: 600; padding: 2px 10px; border-radius: 50px; margin-top: 2px; display: inline-block; color: #fff; }
  .plan-tag.free-tag { background: rgba(6,182,212,.15); color: var(--pink); }
  .plan-tag.prem-tag { background: linear-gradient(90deg, var(--gold), var(--pink)); color: #fff; }

  .plan-features { list-style: none; display: flex; flex-direction: column; gap: 8px; }
  .plan-features li { display: flex; align-items: flex-start; gap: 8px; font-size: 13px; color: var(--muted); line-height: 1.45; }
  .plan-features li::before { content: '✓'; color: var(--gold); font-weight: 700; margin-top: 1px; flex-shrink: 0; }

  /* ── CTA BOTTOM ── */
  .cta-bottom { padding: 32px 20px 48px; text-align: center; background: transparent; }
  .cta-bottom h3 { font-family: 'Bebas Neue', sans-serif; font-size: 28px; letter-spacing: 1.5px; margin-bottom: 8px; color: var(--text); }
  .cta-bottom p { font-size: 13px; color: var(--muted); margin-bottom: 22px; }
  .join-btn-bottom {
    display: inline-flex; align-items: center; gap: 8px; background: linear-gradient(135deg, var(--gold), #5b21b6);
    color: #fff; text-decoration: none; font-weight: 700; font-size: 16px; padding: 15px 36px; border-radius: 50px;
    box-shadow: 0 3px 8px rgba(124,58,237,0.35); transition: all .3s ease;
  }
  .join-btn-bottom:hover { transform: translateY(-2px); box-shadow: 0 10px 28px rgba(6,182,212,0.45); }

  /* ── FOOTER ── */
  footer { text-align: center; padding: 25px 20px; font-size: 15px; color: var(--muted); background: var(--surface); border-top: 1px solid var(--border); }

  @media (max-width: 768px) { footer { padding: 18px 15px; font-size: 13px; } }

  @keyframes floatLogo { 0%,100% { transform: translateY(0); } 50% { transform: translateY(-10px); } }

  .fade-page { animation: pageFade .7s ease; }
  @keyframes pageFade { from { opacity:0; transform:translateY(15px); } to { opacity:1; transform:translateY(0); } }

  #particles-js { position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: -1; pointer-events: none; }
  .reveal { opacity: 0; transform: translateY(30px); transition: all 0.8s ease; }
  .reveal.active { opacity: 1; transform: translateY(0); }

  .header-center { display: block; color: #fff; }
  @media (max-width: 768px) { .header-center { display: none; } }

  :root {
    --bg: #f7f5fb;
    --surface: #ffffff;
    --text: #14101f;
    --border: #e9e3f5;
    --accent: #7c3aed;
    --muted: #6b6178;
  }

  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0d0714;
      --surface: #150e22;
      --text: #ffffff;
      --border: rgba(255,255,255,0.08);
      --accent: #a78bfa;
      --muted: #b5aec2;
    }
  }


  /* ── WATCH / PLAYER ── */
  .watch-wrap { display: flex; flex-direction: column; align-items: center; padding: 34px 16px 6px; }
  .player-card {
    width: 100%; max-width: 860px; background: var(--surface); border: 1px solid var(--border);
    border-radius: 18px; overflow: hidden; box-shadow: 0 8px 30px rgba(124,58,237,.18);
  }
  .player-card video, .player-card audio { width: 100%; display: block; background: #000; }
  .player-card video { max-height: 72vh; }
  .player-card audio { padding: 26px 16px; background: transparent; }
  .player-info { padding: 18px 22px 4px; }
  .player-title { font-weight: 700; font-size: 16px; line-height: 1.45; color: var(--text); word-break: break-word; }
  .player-meta { margin-top: 4px; font-size: 13px; color: var(--muted); }
  .player-actions { display: flex; flex-wrap: wrap; gap: 12px; padding: 16px 22px 8px; }
  .player-actions a { flex: 1 1 190px; justify-content: center; text-align: center; }
  .join-btn-hero.dl-btn { padding: 13px 22px; font-size: 15px; }
  .alt-btn {
    display: inline-flex; align-items: center; gap: 8px; text-decoration: none; font-weight: 700; font-size: 15px;
    padding: 13px 22px; border-radius: 50px; color: var(--accent); background: var(--surface);
    border: 1.5px solid var(--accent); transition: all .3s ease;
  }
  .alt-btn:hover { transform: translateY(-2px); box-shadow: 0 8px 22px rgba(6,182,212,.35); }
  .ext-players { padding: 8px 22px 20px; font-size: 13px; color: var(--muted); }
  .ext-players a { color: var(--accent); font-weight: 600; text-decoration: none; margin-left: 12px; }
  .ext-players a:hover { text-decoration: underline; }
  </style>
</head>
<body class="fade-page">

<canvas id="particles-js"></canvas>

<!-- HEADER -->
<header>
    <div class="logo-wrap">
        <a class="logo-icon" href="https://t.me/Mrn_Officialx" target="_blank">
            <img src="https://files.catbox.moe/idl5a5.png" alt="MRN Officialx Logo">
        </a>
        <a class="logo-text" href="https://t.me/Mrn_Officialx" target="_blank">MRN OFFICIALX</a>
    </div>

    <div class="header-center">NEW RELEASES EVERY DAY</div>

    <a class="join-btn-header" href="https://t.me/+MdUPwSnwvP0zN2U1" target="_blank">
        𝐑𝐄𝐀𝐋𝐈𝐓𝐘 𝐒𝐇𝐎𝐖𝐒
    </a>
</header>

<!-- PLAYER -->
<section class="watch-wrap">
  <div class="badge"><span class="badge-dot"></span>▶ Now Watching · __KIND__</div>
  <div class="player-card">
    __MEDIA_TAG__
    <div class="player-info">
      <div class="player-title">__FILE_NAME__</div>
      <div class="player-meta">__FILE_SIZE__</div>
    </div>
    <div class="player-actions">
      <a class="join-btn-hero dl-btn" href="__DOWNLOAD_URL__">🚀 Fast Download</a>
      <a class="alt-btn" href="__INLINE_URL__" target="_blank" rel="noopener">🔗 Direct Stream Link</a>
    </div>
    <div class="ext-players">
      Open externally:
      <a href="intent:__INLINE_URL__#Intent;action=android.intent.action.VIEW;type=video/*;package=com.mxtech.videoplayer.ad;end">MX Player</a>
      <a href="intent:__INLINE_URL__#Intent;action=android.intent.action.VIEW;type=video/*;package=org.videolan.vlc;end">VLC</a>
      <a href="playit://playerv2/video?url=__INLINE_URL__">PLAYit</a>
    </div>
  </div>
</section>

<!-- HERO -->
<section class="hero reveal">
    <a class="icon-row" href="https://t.me/Mrn_Officialx" target="_blank">
        <img src="https://files.catbox.moe/idl5a5.png" alt="MRN Officialx Logo">
    </a>

    <div class="badge">
        <span class="badge-dot"></span>
        ⭐ Free &amp; Premium Entertainment Service
    </div>

    <div class="hero-title">Unlimited Entertainment</div>
    <div class="hero-subtitle">MRN Officialx</div>

    <p class="hero-desc">
        Access the latest Movies, Web Series, and TV Shows through our community.
        Users can access content for free through shortened links, while Premium members get direct access to videos through our Premium Channel.
    </p>

    <a class="join-btn-hero" href="https://t.me/MRN_Movies_Request_Group" target="_blank">
        𝐌𝐑𝐍 | 𝐌𝐎𝐕𝐈𝐄𝐒 𝐑𝐄𝐐𝐔𝐄𝐒𝐓 𝐆𝐑𝐎𝐔𝐏 →
    </a>
</section>

<!-- STATS -->
<div class="stats-strip reveal">
    <div class="stat"><div class="stat-num">1000+</div><div class="stat-label">Titles Available</div></div>
    <div class="stat"><div class="stat-num">24/7</div><div class="stat-label">Bot Active</div></div>
    <div class="stat"><div class="stat-num">Daily</div><div class="stat-label">New Uploads</div></div>
</div>

<!-- GENRE TAGS -->
<div class="genres">
  <span class="genre-tag">🎬 Movies</span>
  <span class="genre-tag">📺 Web Series</span>
  <span class="genre-tag">📡 TV Shows</span>
</div>

<!-- ABOUT LABEL -->
<div class="section-label"><span>About</span></div>

<!-- ABOUT -->
<section class="about reveal">
  <h2>About MRN Officialx</h2>
  <p>
    MRN Officialx provides Movies, Web Series, and TV Shows through both Free and Premium methods.
    Users can access content for free through shortened links, while
    Premium members receive direct videos through our Premium Channel. New content is added regularly
    to keep the collection updated.
  </p>
</section>

<!-- PLAN CARDS -->
<div class="section-label"><span>How It Works</span></div>
<div class="plans reveal">
  <div class="plan-card free">
    <div class="plan-header">
      <div class="plan-icon free-icon">🔗</div>
      <div>
        <div class="plan-title">Free Access</div>
        <span class="plan-tag free-tag">Always Free</span>
      </div>
    </div>
    <ul class="plan-features">
      <li>Get Content Through Shortened Links</li>
      <li>No Payment Required</li>
      <li>Files Delivered Through Our File Storage Bot</li>
      <li>Regular Content Updates</li>
    </ul>
  </div>

  <div class="plan-card premium">
    <div class="plan-header">
      <div class="plan-icon prem-icon">👑</div>
      <div>
        <div class="plan-title">Premium Access</div>
        <span class="plan-tag prem-tag">⭐ Premium</span>
      </div>
    </div>
    <ul class="plan-features">
      <li>Direct Videos Through Our Premium Channel</li>
      <li>No Shortened Links</li>
      <li>Choose Your Preferred Subscription Duration</li>
      <li>Instant Access After Subscription</li>
    </ul>
  </div>
</div>

<!-- BOTTOM CTA -->
<div class="cta-bottom">
  <h3>Ready to Start Watching?</h3>
  <p>Join MRN Officialx and enjoy Movies, Web Series and TV Shows through Free and Premium access options.</p>
  <a class="join-btn-bottom" href="https://t.me/Mrn_Officialx" target="_blank">
    ✈️ Join Channel →
  </a>
</div>

<!-- FOOTER -->
<footer>
  © 2026 MRN Officialx &nbsp;•&nbsp; 🎬 Movies &nbsp;•&nbsp; 📺 Web Series &nbsp;•&nbsp; 📡 TV Shows
</footer>

<script>
const canvas = document.getElementById("particles-js");
const ctx = canvas.getContext("2d");

let w, h;
let particles = [];

function resize(){
  w = canvas.width = window.innerWidth;
  h = canvas.height = window.innerHeight;
}

class Particle{
  constructor(){
    this.x = Math.random()*w;
    this.y = Math.random()*h;
    this.size = Math.random()*2 + 0.5;
    this.speedX = (Math.random()-0.5)*0.5;
    this.speedY = (Math.random()-0.5)*0.5;
    this.hueMix = Math.random();
  }
  update(){
    this.x += this.speedX;
    this.y += this.speedY;

    if(this.x<0 || this.x>w) this.speedX *= -1;
    if(this.y<0 || this.y>h) this.speedY *= -1;
  }
  draw(){
    const r = Math.round(124 + (6 - 124) * this.hueMix);
    const g = Math.round(58 + (182 - 58) * this.hueMix);
    const b = Math.round(237 + (212 - 237) * this.hueMix);
    ctx.fillStyle = `rgba(${r},${g},${b},0.3)`;
    ctx.beginPath();
    ctx.arc(this.x,this.y,this.size,0,Math.PI*2);
    ctx.fill();
  }
}

function init(){
  particles = [];
  for(let i=0;i<80;i++){
    particles.push(new Particle());
  }
}

function animate(){
  ctx.clearRect(0,0,w,h);
  particles.forEach(p=>{
    p.update();
    p.draw();
  });
  requestAnimationFrame(animate);
}

window.addEventListener("resize", ()=>{
  resize();
  init();
});

resize();
init();
animate();


// SCROLL REVEAL
function reveal(){
  let elements = document.querySelectorAll(".reveal");

  elements.forEach(el=>{
    let top = el.getBoundingClientRect().top;

    if(top < window.innerHeight - 100){
      el.classList.add("active");
    }
  });
}

window.addEventListener("scroll", reveal);
reveal();
</script>

</body>
</html>
"""


_WATCH_TOKEN_RE = re.compile(r"__(FILE_NAME|FILE_SIZE|MEDIA_TAG|DOWNLOAD_URL|INLINE_URL|KIND)__")


async def render_watch_page(chat_id: int, message_id: int) -> str:
    # NOTE: the link's signature + expiry must already be verified by the
    # caller (via resolve_stream_token) before this is ever called.
    streamer = get_streamer()
    file_id = await streamer.get_file_properties(chat_id, message_id)

    issued_at = int(time.time())
    download_url = _dl_url(chat_id, message_id, file_id.file_name, force_download=True, issued_at=issued_at)
    inline_url = _dl_url(chat_id, message_id, file_id.file_name, force_download=False, issued_at=issued_at)
    mime_type = file_id.mime_type or ""
    # html.escape: file_name comes straight from the uploaded file's own
    # filename, which an attacker-controlled upload (public mode) could set
    # to something like '<img src=x onerror=...>' - never trust it unescaped
    # in HTML output. URLs are escaped too (they sit inside attributes).
    file_name = html.escape((file_id.file_name or "file").replace("_", " "))
    file_size = humanbytes(file_id.file_size)
    safe_inline = html.escape(inline_url, quote=True)
    safe_download = html.escape(download_url, quote=True)

    if mime_type.startswith("audio"):
        kind = "Audio"
        media_tag = f'<audio controls preload="metadata" src="{safe_inline}"></audio>'
    else:
        kind = "Video"
        media_tag = f'<video controls playsinline preload="metadata" src="{safe_inline}"></video>'

    values = {
        "FILE_NAME": file_name, "FILE_SIZE": file_size, "MEDIA_TAG": media_tag,
        "DOWNLOAD_URL": safe_download, "INLINE_URL": safe_inline, "KIND": kind,
    }
    # single pass, so a file name that happens to contain a token like
    # __INLINE_URL__ can never be substituted a second time
    return _WATCH_TOKEN_RE.sub(lambda m: values[m.group(1)], _WATCH_PAGE)
