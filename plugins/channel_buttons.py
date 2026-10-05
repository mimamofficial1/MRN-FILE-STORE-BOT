"""Auto 'Fast Download' / 'Watch Online' buttons under media posted in a channel.

Setup (once per channel):
  1. Make the bot an admin of the channel with 'Edit messages of others'
     (and 'Delete messages of others' so it can tidy up its own command).
  2. Post  /autobutton on  in that channel.  (/autobutton off  to stop,
     /autobutton  alone shows the current status.)

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
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import LOG_CHANNEL, STREAM_MODE
from plugins.dbusers import db
from TechVJ.server.streamer import build_permanent_urls, get_media_from_message

logger = logging.getLogger(__name__)

_col = db.db.auto_button_channels
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
    global _edit_lock
    if _edit_lock is None:
        _edit_lock = asyncio.Lock()
    async with _edit_lock:  # one edit at a time -> no flood when many files are posted
        for _ in range(2):
            try:
                await message.edit_reply_markup(markup)
                break
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(
                    f"[AUTO-BUTTON] could not edit post {message.id} in {message.chat.id}: "
                    f"{type(e).__name__}: {e}  (bot needs admin + 'Edit messages of others')"
                )
                break
        await asyncio.sleep(0.4)


@Client.on_message(filters.channel & (filters.video | filters.document | filters.audio), group=3)
async def channel_media_buttons(client, message):
    try:
        if not STREAM_MODE or message.chat.id == LOG_CHANNEL or message.reply_markup:
            return
        if not await _is_enabled(message.chat.id):
            return
        media = get_media_from_message(message)
        if not media:
            return
        download_url, watch_url = build_permanent_urls(
            message.chat.id, message.id, getattr(media, "file_name", "") or "file"
        )
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton('🚀 ꜰᴀꜱᴛ ᴅᴏᴡɴʟᴏᴀᴅ 🚀', url=download_url),
            InlineKeyboardButton('🖥 ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ 🖥', url=watch_url),
        ]])
        _spawn(_apply_buttons(message, markup))
    except Exception:
        logger.error("[AUTO-BUTTON] handler failed", exc_info=True)
