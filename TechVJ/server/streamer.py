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


_WATCH_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{file_name} | Mrn Officialx</title>
<style>
  :root {{
    --bg: #05070d; --panel: rgba(20,24,38,.75); --border: rgba(255,255,255,.08);
    --text: #f4f6fb; --muted: #98a2b8; --gold: #f5c453; --rose: #f43f5e;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; min-height: 100vh; font-family: 'Segoe UI', Roboto, Arial, sans-serif;
    background: radial-gradient(circle at top, #131a2c, var(--bg) 65%);
    color: var(--text); display: flex; flex-direction: column; align-items: center;
    padding: 24px 14px 48px;
  }}
  .brand {{
    font-weight: 800; letter-spacing: .5px; margin-bottom: 18px; font-size: 1.15rem;
    background: linear-gradient(120deg, var(--gold), var(--rose));
    -webkit-background-clip: text; background-clip: text; -webkit-text-fill-color: transparent;
  }}
  .card {{
    width: 100%; max-width: 780px; background: var(--panel); border: 1px solid var(--border);
    border-radius: 16px; overflow: hidden; backdrop-filter: blur(10px);
  }}
  video, audio {{ width: 100%; display: block; background: #000; }}
  audio {{ padding: 28px 16px; }}
  .info {{ padding: 18px 20px 6px; }}
  .title {{ font-size: 1.02rem; font-weight: 600; word-break: break-word; }}
  .meta {{ margin-top: 4px; color: var(--muted); font-size: .85rem; }}
  .actions {{ display: flex; flex-wrap: wrap; gap: 10px; padding: 16px 20px 22px; }}
  .btn {{
    flex: 1 1 150px; text-align: center; text-decoration: none; padding: 12px 14px;
    border-radius: 10px; font-weight: 600; font-size: .9rem; border: 1px solid var(--border);
    color: var(--text); background: rgba(255,255,255,.04); transition: transform .15s ease;
  }}
  .btn:active {{ transform: scale(.97); }}
  .btn.primary {{ background: linear-gradient(120deg, var(--gold), var(--rose)); color: #10131d; border: none; }}
  .players {{ padding: 0 20px 26px; color: var(--muted); font-size: .82rem; }}
  .players a {{ color: var(--gold); text-decoration: none; margin-right: 12px; }}
</style>
</head>
<body>
  <div class="brand">⚡ Mrn Officialx</div>
  <div class="card">
    {media_tag}
    <div class="info">
      <div class="title">{file_name}</div>
      <div class="meta">{file_size}</div>
    </div>
    <div class="actions">
      <a class="btn primary" href="{download_url}">🚀 Fast Download</a>
      <a class="btn" href="{inline_url}" target="_blank" rel="noopener">🔗 Direct Stream Link</a>
    </div>
    <div class="players">
      Open externally:
      <a href="intent:{inline_url}#Intent;action=android.intent.action.VIEW;type=video/*;package=com.mxtech.videoplayer.ad;end">MX Player</a>
      <a href="intent:{inline_url}#Intent;action=android.intent.action.VIEW;type=video/*;package=org.videolan.vlc;end">VLC</a>
      <a href="playit://playerv2/video?url={inline_url}">PLAYit</a>
    </div>
  </div>
</body>
</html>"""


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
    # in HTML output.
    file_name = html.escape((file_id.file_name or "file").replace("_", " "))
    file_size = humanbytes(file_id.file_size)

    if mime_type.startswith("audio"):
        media_tag = f'<audio controls preload="metadata" src="{inline_url}"></audio>'
    else:
        media_tag = f'<video controls playsinline preload="metadata" src="{inline_url}"></video>'

    return _WATCH_PAGE.format(
        file_name=file_name, file_size=file_size, media_tag=media_tag,
        download_url=download_url, inline_url=inline_url,
    )
