# Premium (animated custom) emoji for the WHOLE bot - this one file, nothing else is edited.
#
# How it works
#   Every message / caption / edit the bot sends goes through Client.invoke().
#   This plugin hooks there: it finds the normal emoji in the text (✅ 📁 ⚡ ...)
#   and, for the ones you mapped, adds a custom-emoji entity so users see your
#   premium emoji instead. All existing texts in all files work unchanged.
#
# Telegram rule (Bot API 9.4, Feb 2026): a bot can send premium emoji in private,
# group and supergroup chats if the BOT'S OWNER (the account that created the bot in
# @BotFather) has Telegram Premium. Nothing else is needed - NO STRING_SESSION.
# Not supported by Telegram: channels (the LOG_CHANNEL is skipped), inline mode, and
# emoji inside buttons (this pyrofork version cannot do button icons).
#
# Setup (admin, in the bot's private chat):
#   1. /setemoji  + premium emoji in the same message (or reply to such a message).
#        Example: send   /setemoji ✅ 📁 ⚡   where each one is a PREMIUM emoji.
#        The standard emoji it replaces is learned automatically. Repeat as needed.
#   2. /emojion   -> sends a test with your first emoji; turns ON only if Telegram accepts it.
#   /emojioff  /emojilist  /delemoji <emoji or all>  /emoji (status)
#
# Safety: if Telegram ever refuses a message because of the emoji (e.g. the owner's
# Premium expired), the message is re-sent with normal emoji at once, and after 3 such
# failures in a row the feature switches itself off. Nothing breaks.

import logging
import re
import time

from pyrogram import Client, enums, filters, raw
from pyrogram.errors import BadRequest
from pyrogram.types import Message
from pyrogram.utils import get_channel_id

from config import LOG_CHANNEL
from plugins.admins_db import dynamic_admin_filter
from plugins.settings_db import get_settings, update_setting

logger = logging.getLogger(__name__)

MAX_PER_MESSAGE = 80
MAX_MAPPINGS = 300
AUTO_OFF_AFTER = 3
_FE0F = "\ufe0f"

# BadRequest errors that have nothing to do with emoji: never retried
_UNRELATED = {
    "MESSAGE_NOT_MODIFIED", "PEER_ID_INVALID", "INPUT_USER_DEACTIVATED", "USER_IS_BLOCKED",
    "CHAT_WRITE_FORBIDDEN", "MESSAGE_ID_INVALID", "CHAT_ID_INVALID", "CHANNEL_INVALID",
    "USER_DEACTIVATED", "MESSAGE_DELETE_FORBIDDEN", "REPLY_MARKUP_INVALID", "BUTTON_URL_INVALID",
    "BUTTON_DATA_INVALID", "MEDIA_EMPTY", "MESSAGE_EMPTY", "PEER_FLOOD", "BOT_RESPONSE_TIMEOUT",
}

_state = {"loaded": False, "on": False, "map": {}, "rx": None, "fails": 0}

try:
    _lc = int(LOG_CHANNEL)
    _log_channel_id = get_channel_id(_lc) if _lc < -1000000000000 else None
except Exception:
    _log_channel_id = None


def _u16(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def _rebuild():
    keys = sorted(_state["map"], key=len, reverse=True)
    if not keys:
        _state["rx"] = None
        return
    # never cut a ZWJ sequence (👨‍💻) or a skin-tone emoji in half
    pat = "|".join("(?<!\u200d)" + re.escape(k) + _FE0F + "?(?![\u200d\U0001F3FB-\U0001F3FF])" for k in keys)
    _state["rx"] = re.compile(pat)


async def _load_state():
    if time.monotonic() < _state.get("retry_at", 0):
        return
    try:
        s = await get_settings()
    except Exception:
        _state["retry_at"] = time.monotonic() + 30   # database not ready: don't hammer it, retry in 30s
        return
    _state["on"] = bool(s.get("premium_emoji", False))
    m = {}
    for pair in s.get("premium_emoji_map") or []:
        try:
            m[str(pair[0])] = int(pair[1])
        except Exception:
            continue
    _state["map"] = m
    _rebuild()
    _state["loaded"] = True


async def _save_map():
    await update_setting("premium_emoji_map", [[k, str(v)] for k, v in _state["map"].items()])


def _inject(text, entities):
    """Returns a new entity list with premium emoji added, or None if nothing to add."""
    rx = _state["rx"]
    if not text or rx is None:
        return None
    entities = list(entities or [])
    blocked = [
        (e.offset, e.offset + e.length)
        for e in entities
        if isinstance(e, (raw.types.MessageEntityCustomEmoji, raw.types.MessageEntityCode, raw.types.MessageEntityPre))
    ]
    mapping = _state["map"]
    added, pos16, last = [], 0, 0
    for m in rx.finditer(text):
        pos16 += _u16(text[last:m.start()])
        last = m.start()
        doc_id = mapping.get(m.group().replace(_FE0F, ""))
        length = _u16(m.group())
        if doc_id is None or any(a <= pos16 < b for a, b in blocked):
            continue
        added.append(raw.types.MessageEntityCustomEmoji(offset=pos16, length=length, document_id=doc_id))
        if len(added) >= MAX_PER_MESSAGE:
            break
    if not added:
        return None
    return sorted(entities + added, key=lambda e: e.offset)


def _skip_peer(peer) -> bool:
    return (
        _log_channel_id is not None
        and isinstance(peer, raw.types.InputPeerChannel)
        and peer.channel_id == _log_channel_id
    )


def _targets(query):
    f = raw.functions.messages
    t = type(query)
    if t in (f.SendMessage, f.SendMedia, f.EditMessage):
        return [] if _skip_peer(query.peer) else [query]
    if t is f.SendMultiMedia:
        return [] if _skip_peer(query.peer) else list(query.multi_media)
    return []


_orig_invoke = Client.invoke


async def _invoke(self, query, *args, **kwargs):
    targets = _targets(query)
    if not targets:                      # login, downloads, reactions, ... : not our business
        return await _orig_invoke(self, query, *args, **kwargs)
    if not _state["loaded"]:
        await _load_state()
    undo = []
    if _state["on"] and _state["rx"] is not None:
        for tgt in targets:
            new = _inject(getattr(tgt, "message", None), getattr(tgt, "entities", None))
            if new is not None:
                undo.append((tgt, tgt.entities))
                tgt.entities = new
    if not undo:
        return await _orig_invoke(self, query, *args, **kwargs)
    try:
        result = await _orig_invoke(self, query, *args, **kwargs)
        _state["fails"] = 0
        return result
    except BadRequest as e:
        if getattr(e, "ID", "") in _UNRELATED:
            raise
        # maybe Telegram refused the premium emoji (owner Premium expired?) - try plain
        for tgt, old in undo:
            tgt.entities = old
        result = await _orig_invoke(self, query, *args, **kwargs)   # raises again if it was something else
        _state["fails"] += 1
        logger.warning(f"[premium_emoji] Telegram refused premium emoji ({getattr(e, 'ID', e)}); sent plain instead "
                       f"({_state['fails']}/{AUTO_OFF_AFTER})")
        if _state["fails"] >= AUTO_OFF_AFTER:
            _state["on"] = False
            logger.error("[premium_emoji] switched OFF automatically - does the bot owner still have Telegram Premium?")
            try:
                await update_setting("premium_emoji", False)
            except Exception:
                pass
        return result


if not getattr(Client.invoke, "_premium_patched", False):
    _invoke._premium_patched = True
    Client.invoke = _invoke


# --------------------------------------------------------------------------- admin commands
_ADMIN = dynamic_admin_filter("can_manage_admins")


def _learn_from(m: Message) -> dict:
    text = m.text or m.caption or ""
    ents = m.entities or m.caption_entities or []
    u = str(text).encode("utf-16-le")
    out = {}
    for e in ents:
        if e.type != enums.MessageEntityType.CUSTOM_EMOJI:
            continue
        ch = u[e.offset * 2:(e.offset + e.length) * 2].decode("utf-16-le", "ignore").replace(_FE0F, "").strip()
        if ch and e.custom_emoji_id:
            out[ch] = int(e.custom_emoji_id)
    return out


@Client.on_message(filters.command("setemoji") & filters.private & _ADMIN)
async def set_emoji_cmd(client, message: Message):
    await _load_state() if not _state["loaded"] else None
    learned = _learn_from(message)
    if message.reply_to_message:
        learned.update(_learn_from(message.reply_to_message))
    if not learned:
        return await message.reply_text(
            "<b>Premium emoji kaise set karein:</b>\n\n"
            "<code>/setemoji</code> ke saath apne <b>premium emoji</b> bhejo (same message mein), "
            "ya kisi premium emoji wale message ko reply karke <code>/setemoji</code> likho.\n\n"
            "Har premium emoji apne normal emoji ki jagah le leta hai (jaise premium ✅ → normal ✅).\n"
            "Phir <code>/emojion</code> se chalu karo."
        )
    if len(_state["map"]) + len(learned) > MAX_MAPPINGS:
        return await message.reply_text(f"<b>❌ Max {MAX_MAPPINGS} emoji mappings allowed.</b>")
    _state["map"].update(learned)
    _rebuild()
    await _save_map()
    await message.reply_text(
        f"<b>✅ {len(learned)} emoji saved</b> (total {len(_state['map'])}).\n"
        + ("Ab <code>/emojion</code> bhejo." if not _state["on"] else "Ye turant lagu ho gaye.")
    )


@Client.on_message(filters.command("emojion") & filters.private & _ADMIN)
async def emoji_on_cmd(client, message: Message):
    await _load_state() if not _state["loaded"] else None
    if not _state["map"]:
        return await message.reply_text("<b>Pehle <code>/setemoji</code> se kuch premium emoji set karo.</b>")
    ch, doc_id = next(iter(_state["map"].items()))
    try:
        # explicit entity, so this test works whatever the current switch state is
        await client.send_message(
            message.chat.id,
            f'<emoji id="{doc_id}">{ch}</emoji> Premium emoji test',
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        _state["on"] = False
        return await message.reply_text(
            "<b>❌ Telegram ne premium emoji accept nahi kiya.</b>\n\n"
            "Bot ke <b>owner account</b> (jisse BotFather mein bot banaya) ke paas Telegram Premium hona chahiye. "
            "Bot ka owner BotFather mein bot ki settings mein dikhta hai.\n\n"
            f"<code>{type(e).__name__}: {e}</code>"
        )
    _state["on"] = True
    _state["fails"] = 0
    await update_setting("premium_emoji", True)
    await message.reply_text("<b>✅ Premium emoji ON.</b> Test message mein animated emoji dikh raha hai to sab theek hai.")


@Client.on_message(filters.command("emojioff") & filters.private & _ADMIN)
async def emoji_off_cmd(client, message: Message):
    _state["on"] = False
    await update_setting("premium_emoji", False)
    await message.reply_text("<b>Premium emoji OFF.</b> Mappings saved hain, <code>/emojion</code> se wapas chalu karo.")


@Client.on_message(filters.command("delemoji") & filters.private & _ADMIN)
async def del_emoji_cmd(client, message: Message):
    await _load_state() if not _state["loaded"] else None
    arg = (message.text or "").split(None, 1)[1].strip() if len((message.text or "").split(None, 1)) > 1 else ""
    if not arg:
        return await message.reply_text("<b>Usage:</b> <code>/delemoji ✅ 📁</code> ya <code>/delemoji all</code>")
    if arg.lower() == "all":
        n = len(_state["map"])
        _state["map"].clear()
    else:
        n = 0
        for ch in list(_state["map"]):
            if ch in arg.replace(_FE0F, ""):
                del _state["map"][ch]
                n += 1
    _rebuild()
    await _save_map()
    await message.reply_text(f"<b>🗑 {n} emoji removed</b> ({len(_state['map'])} left).")


@Client.on_message(filters.command(["emoji", "emojilist"]) & filters.private & _ADMIN)
async def emoji_status_cmd(client, message: Message):
    await _load_state() if not _state["loaded"] else None
    m = _state["map"]
    listing = " ".join(m) if m else "—"
    await message.reply_text(
        f"<b>Premium emoji:</b> {'ON ✅' if _state['on'] else 'OFF ❌'}\n"
        f"<b>Mapped:</b> {len(m)}\n\n{listing}\n\n"
        "<code>/setemoji</code> · <code>/delemoji</code> · <code>/emojion</code> · <code>/emojioff</code>"
    )
