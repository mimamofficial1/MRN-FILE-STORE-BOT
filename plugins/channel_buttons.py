"""Auto 'Fast Download' / 'Watch Online' buttons under media posted in a channel.

Setup (once per channel):
  1. Make the bot an admin of the channel with 'Edit messages of others'
     (and 'Delete messages of others' so it can tidy up its own command).
  2. Post  /autobutton on  in that channel.  (/autobutton off  to stop,
     /autobutton  alone shows the current status.)

  3. /autobutton fix  (or  /autobutton fix 200 ) adds the buttons to posts that are
     still missing them - looks at the last 50 (max 500) posts. Posts made while the bot
     was restarting are also caught up automatically when it comes back.

From then on every video / document / audio posted in that channel gets the
two buttons added automatically. The buttons use never-expiring signed links
(see build_permanent_urls) that redirect to a fresh stream link on every click.

Optional: AUTO_BUTTON_CHANNELS="-100123.. -100456.." env var enables channels
without the command.
"""
import asyncio
import logging
import os

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import LOG_CHANNEL, STREAM_MODE
from plugins.dbusers import db
from TechVJ.server.streamer import build_permanent_urls, get_media_from_message

logger = logging.getLogger(__name__)

_col = db.db.auto_button_channels
_lastcol = db.db.auto_button_last   # newest post id handled per channel (for catch-up after a restart)
_last = {}                          # chat_id -> newest post id handled
_edit_gap = 0.25                    # pause between edits; doubles on FloodWait, eases back after
_env_ids = {int(x) for x in os.environ.get("AUTO_BUTTON_CHANNELS", "").split() if x.lstrip("-").isdigit()}
_enabled = None  # set of chat ids, loaded lazily from MongoDB
_edit_lock = None
_BG = set()


def _spawn(coro):
    t = asyncio.create_task(coro)
    _BG.add(t)
    t.add_done_callback(_BG.discard)
    return t


async def _load_enabled():
    global _enabled
    if _enabled is None:
        try:
            _enabled = {d["_id"] async for d in _col.find({})}
        except Exception as e:
            logger.error(f"[AUTO-BUTTON] could not load channels from DB: {e}")
            return set()  # try again next time
    return _enabled


async def _is_enabled(chat_id):
    if chat_id in _env_ids:
        return True
    return chat_id in await _load_enabled()


async def _set_enabled(chat_id, on):
    enabled = await _load_enabled()
    if on:
        await _col.update_one({"_id": chat_id}, {"$set": {"_id": chat_id}}, upsert=True)
        enabled.add(chat_id)
    else:
        await _col.delete_one({"_id": chat_id})
        enabled.discard(chat_id)


async def _tidy(client, chat_id, ids, delay):
    await asyncio.sleep(delay)
    try:
        await client.delete_messages(chat_id, ids)
    except Exception:
        pass  # no 'delete messages' right - harmless


@Client.on_message(filters.channel & filters.command("autobutton"))
async def autobutton_command(client, message):
    # Only channel admins can post in a channel, so posting here = authorised.
    chat_id = message.chat.id
    arg = message.command[1].lower() if len(message.command) > 1 else ""
    try:
        if arg == "fix":
            if not STREAM_MODE:
                text = "⚠️ STREAM_MODE is False on the bot, so no buttons can be added."
            elif not await _is_enabled(chat_id):
                text = "ℹ️ Auto buttons are OFF for this channel. Use /autobutton on first."
            else:
                try:
                    n = min(max(int(message.command[2]), 1), 500) if len(message.command) > 2 else 50
                except ValueError:
                    n = 50
                reply = await client.send_message(chat_id, f"⏳ Checking the last {n} posts...")
                ids = range(max(message.id - n, 1), message.id)
                checked, fixed = await _scan(client, chat_id, ids)
                await reply.edit_text(f"✅ Checked {checked} posts, added buttons to {fixed}.")
                _spawn(_tidy(client, chat_id, [message.id, reply.id], 12))
                return
            reply = await client.send_message(chat_id, text)
            _spawn(_tidy(client, chat_id, [message.id, reply.id], 8))
            return
        if arg in ("on", "off"):
            await _set_enabled(chat_id, arg == "on")
            text = ("✅ Auto buttons are now ON for this channel. New videos/files will get "
                    "Fast Download + Watch Online buttons." if arg == "on"
                    else "🛑 Auto buttons are now OFF for this channel.")
        else:
            on = await _is_enabled(chat_id)
            text = f"ℹ️ Auto buttons are {'ON' if on else 'OFF'} for this channel.\nUse /autobutton on or /autobutton off."
        if not STREAM_MODE:
            text += "\n\n⚠️ STREAM_MODE is False on the bot, so no buttons will be added."
        reply = await client.send_message(chat_id, text)
        _spawn(_tidy(client, chat_id, [message.id, reply.id], 8))
    except Exception as e:
        logger.error(f"[AUTO-BUTTON] /autobutton failed in {chat_id}: {type(e).__name__}: {e}")


async def _apply_buttons(message, markup):
    """Edit the buttons onto one post. Returns True when they are on. One edit at a time (no
    flood when many files are posted), but now it really keeps trying: a FloodWait is waited
    out and retried (it used to give up silently after the 2nd one), network hiccups too."""
    global _edit_lock, _edit_gap
    if _edit_lock is None:
        _edit_lock = asyncio.Lock()
    async with _edit_lock:
        ok = False
        for attempt in range(6):
            try:
                await message.edit_reply_markup(markup)
                ok = True
                _edit_gap = max(0.25, _edit_gap * 0.9)
                break
            except MessageNotModified:
                ok = True   # already has exactly these buttons
                break
            except FloodWait as e:
                _edit_gap = min(2.0, _edit_gap * 2)
                logger.warning(f"[AUTO-BUTTON] FloodWait {e.value}s on {message.chat.id}, waiting")
                await asyncio.sleep(e.value + 1)
            except (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError) as e:
                logger.warning(f"[AUTO-BUTTON] network problem editing post {message.id}: {type(e).__name__}, retrying")
                await asyncio.sleep(1 + attempt)
            except Exception as e:
                logger.error(
                    f"[AUTO-BUTTON] could not edit post {message.id} in {message.chat.id}: "
                    f"{type(e).__name__}: {e}  (bot needs admin + 'Edit messages of others')"
                )
                break
        else:
            logger.error(f"[AUTO-BUTTON] gave up on post {message.id} in {message.chat.id} after 6 tries")
        await asyncio.sleep(_edit_gap)
        return ok


def _wants_buttons(message) -> bool:
    if not STREAM_MODE or message.empty or message.chat.id == LOG_CHANNEL or message.reply_markup:
        return False
    return bool(message.video or message.document or message.audio)


def _needs_repair(message) -> bool:
    """Our own two buttons on a post, but without their emoji (an earlier version stripped the emoji from
    the labels of channel buttons). They are rebuilt with the proper labels."""
    if os.environ.get("PREMIUM_CHANNEL_BUTTONS", "").strip() in ("1", "true", "yes"):
        return False   # premium icons ON for channels: the label text read back has no emoji on purpose
    rows = getattr(getattr(message, "reply_markup", None), "inline_keyboard", None)
    if not rows or message.empty or not (message.video or message.document or message.audio):
        return False
    texts = [getattr(b, "text", "") for row in rows for b in row]
    return (len(texts) == 2 and "ꜰᴀꜱᴛ ᴅᴏᴡɴʟᴏᴀᴅ" in texts[0] and "ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ" in texts[1]
            and not any(e in t for t in texts for e in ("🚀", "🖥")))


def _markup_for(message):
    media = get_media_from_message(message)
    if not media:
        return None
    download_url, watch_url = build_permanent_urls(
        message.chat.id, message.id, getattr(media, "file_name", "") or "file"
    )
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('🚀 ꜰᴀꜱᴛ ᴅᴏᴡɴʟᴏᴀᴅ 🚀', url=download_url),
        InlineKeyboardButton('🖥 ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ 🖥', url=watch_url),
    ]])


async def _note_seen(chat_id, msg_id):
    """Remember the newest post we handled, so after a restart we know where to catch up from."""
    if msg_id <= _last.get(chat_id, 0):
        return
    _last[chat_id] = msg_id
    try:
        await _lastcol.update_one({"_id": chat_id}, {"$max": {"last": msg_id}}, upsert=True)
    except Exception:
        pass


async def _get_msgs(client, chat_id, ids):
    for _ in range(3):
        try:
            return await client.get_messages(chat_id, ids)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
    return []


async def _scan(client, chat_id, ids):
    """Check these post ids and put the buttons on the ones that lack them.
    Returns (posts_checked, posts_fixed)."""
    checked = fixed = 0
    ids = list(ids)
    for i in range(0, len(ids), 100):
        for m in await _get_msgs(client, chat_id, ids[i:i + 100]):
            if m.empty:
                continue
            checked += 1
            if _wants_buttons(m) or _needs_repair(m):
                markup = _markup_for(m)
                if markup and await _apply_buttons(m, markup):
                    fixed += 1
    return checked, fixed


async def _catch_up_channel(client, chat_id, last):
    """Posts made while the bot was offline/restarting were never delivered to it (Pyrogram skips
    updates from downtime). Look at the ids after the newest one we handled and fix any that
    arrived meanwhile."""
    total_fixed = 0
    for _ in range(10):  # max ~1000 posts per channel per start
        ids = list(range(last + 1, last + 101))
        msgs = await _get_msgs(client, chat_id, ids)
        real = [m for m in msgs if not m.empty]
        if not real:
            break
        for m in real:
            if _wants_buttons(m):
                markup = _markup_for(m)
                if markup and await _apply_buttons(m, markup):
                    total_fixed += 1
        last = max(m.id for m in real)
        _last[chat_id] = max(_last.get(chat_id, 0), last)
        try:
            await _lastcol.update_one({"_id": chat_id}, {"$max": {"last": last}}, upsert=True)
        except Exception:
            pass
    if total_fixed:
        logger.info(f"[AUTO-BUTTON] catch-up: added buttons to {total_fixed} post(s) in {chat_id}")


async def _startup_catch_up():
    try:
        from TechVJ.bot import StreamBot
    except Exception:
        return
    for _ in range(90):  # wait (up to ~3 min) until the bot is fully started
        if StreamBot.is_connected and getattr(StreamBot, "me", None) is not None:
            break
        await asyncio.sleep(2)
    else:
        return
    try:
        channels = set(await _load_enabled()) | _env_ids
        last_seen = {d["_id"]: d.get("last") async for d in _lastcol.find({})}
    except Exception as e:
        logger.error(f"[AUTO-BUTTON] catch-up could not read the database: {e}")
        return
    for chat_id in channels:
        last = last_seen.get(chat_id)
        if not last:
            continue  # never seen a post in this channel yet -> nothing to catch up from
        _last[chat_id] = last
        try:
            await _catch_up_channel(StreamBot, chat_id, last)
            top = _last.get(chat_id, last)
            _checked, repaired = await _scan(StreamBot, chat_id, range(max(1, top - 59), top + 1))
            if repaired:
                logger.info(f"[AUTO-BUTTON] start-up repair: fixed buttons on {repaired} post(s) in {chat_id}")
        except Exception as e:
            logger.error(f"[AUTO-BUTTON] catch-up failed for {chat_id}: {type(e).__name__}: {e}")


try:
    _spawn(_startup_catch_up())   # plugins are loaded while the event loop runs; harmless otherwise
except RuntimeError:
    pass


@Client.on_message(filters.channel & (filters.video | filters.document | filters.audio), group=3)
async def channel_media_buttons(client, message):
    try:
        if message.chat.id == LOG_CHANNEL or not STREAM_MODE:
            return
        if not await _is_enabled(message.chat.id):
            return
        _spawn(_note_seen(message.chat.id, message.id))
        if not _wants_buttons(message):
            return
        markup = _markup_for(message)
        if markup:
            _spawn(_apply_buttons(message, markup))
    except Exception:
        logger.error("[AUTO-BUTTON] handler failed", exc_info=True)
