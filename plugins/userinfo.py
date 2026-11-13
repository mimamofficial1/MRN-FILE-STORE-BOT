# /info - Telegram profile details of a user.
#
#   /info               -> your own details
#   /info 7473323779    -> details of that user id
#   /info @username     -> details of that public username (t.me/username also works)
#   reply to a message with /info -> details of the sender of that message
#
# Only what Telegram itself shows the bot about a profile (name, username, id, DC, Premium, language,
# last-seen group). Looking up a user by numeric id works for people the bot has seen (they started the
# bot or wrote to it); @username works for every public username.

import html
import logging
import re
import time

from pyrogram import Client, enums, filters
from pyrogram.errors import FloodWait

from plugins.dbusers import db

logger = logging.getLogger(__name__)

COOLDOWN = 3.0            # seconds between lookups of OTHER people, per user (keeps get_users from being spammed)
_last_lookup = {}

_STATUS = {
    enums.UserStatus.ONLINE: "Online",
    enums.UserStatus.OFFLINE: "Offline",
    enums.UserStatus.RECENTLY: "Recently",
    enums.UserStatus.LAST_WEEK: "Within a week",
    enums.UserStatus.LAST_MONTH: "Within a month",
    enums.UserStatus.LONG_AGO: "A long time ago",
}
_USERNAME_RX = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")

USAGE = (
    "<b>ℹ️ /info kaise use karein:</b>\n\n"
    "<code>/info</code> - apni details\n"
    "<code>/info 7473323779</code> - user id se\n"
    "<code>/info @username</code> - username se\n"
    "Ya kisi ke message ko <b>reply</b> karke <code>/info</code> likho."
)


def _status_text(user) -> str:
    st = getattr(user, "status", None)
    return _STATUS.get(st, "Hidden") if st is not None else "Hidden"


def build_info(u) -> str:
    e = html.escape
    first = e(u.first_name or "N/A")
    full = e(" ".join(x for x in (u.first_name, u.last_name) if x) or "Deleted Account")
    lines = [
        "<b><u>User Details :</u></b>",
        "",
        f"🦚 <b>First Name :</b> {first}",
    ]
    if getattr(u, "last_name", None):
        lines += ["", f"🦚 <b>Last Name :</b> {e(u.last_name)}"]
    lines += [
        "",
        f"👤 <b>User Id :</b> <code>{u.id}</code>",
        "",
        f"👦 <b>Username :</b> {('@' + e(u.username)) if getattr(u, 'username', None) else 'None'}",
        "",
        f"🔗 <b>User Link :</b> <a href=\"tg://user?id={u.id}\">{full}</a>",
        "",
        f"💬 <b>DC ID :</b> {getattr(u, 'dc_id', None) or 'Unknown'}",
        "",
        f"🌟 <b>Telegram Premium :</b> {bool(getattr(u, 'is_premium', False))}",
        "",
        f"📃 <b>Language Code :</b> {e(getattr(u, 'language_code', None) or 'Unknown')}",
        "",
        f"💫 <b>Status :</b> {_status_text(u)}",
    ]
    if getattr(u, "is_bot", False):
        lines += ["", "🤖 <b>Is Bot :</b> True"]
    if getattr(u, "is_deleted", False):
        lines += ["", "🗑 <b>Deleted Account :</b> True"]
    lines += ["", "If you need user id, Then just tap and copy."]
    return "\n".join(lines)


def _parse_target(arg: str):
    """'7473323779' -> int, '@name' / 't.me/name' / 'name' -> '@name' style username, anything else -> None."""
    arg = arg.strip()
    if arg.lstrip("+").isdigit():
        uid = int(arg)
        return uid if uid > 0 else None
    arg = re.sub(r"^(https?://)?(www\.)?t\.me/", "", arg).lstrip("@").split("?")[0].strip("/")
    return arg if _USERNAME_RX.match(arg) else None


@Client.on_message(filters.command("info") & filters.private)
async def user_info(client, message):
    me = message.from_user
    if me is None:
        return
    row = await db.get_user(me.id)
    if row and row.get("banned", False):
        return await message.reply_text("<b>🚫 You are banned from using this bot.</b>")

    args = message.command[1:]
    reply_user = message.reply_to_message.from_user if message.reply_to_message else None

    if not args and reply_user is None:
        return await message.reply_text(build_info(me))            # your own details, no lookup needed

    if args:
        target = _parse_target(args[0])
        if target is None:
            return await message.reply_text(USAGE)
    else:
        target = reply_user.id

    if target == me.id:
        return await message.reply_text(build_info(me))

    now = time.monotonic()
    if now - _last_lookup.get(me.id, 0) < COOLDOWN:
        return await message.reply_text("<b>⏳ Thoda ruko, 3 second baad dobara try karo.</b>")
    _last_lookup[me.id] = now
    if len(_last_lookup) > 20000:
        _last_lookup.clear()

    try:
        user = await client.get_users(target)
    except FloodWait as e:
        return await message.reply_text(f"<b>⏳ Telegram ne rok diya, {e.value} second baad try karo.</b>")
    except Exception as e:
        kind = type(e).__name__
        logger.info(f"[INFO] lookup of {target!r} failed: {kind}")
        if isinstance(target, int):
            return await message.reply_text(
                "<b>❌ Ye user id nahi mili.</b>\n\n"
                "Bot sirf unhe dekh sakta hai jinhone bot start kiya ho ya bot ko message kiya ho. "
                "Public username (<code>/info @username</code>) se har kisi ki details mil jati hain."
            )
        return await message.reply_text("<b>❌ Ye username nahi mila.</b>")
    if isinstance(user, list):
        user = user[0] if user else None
    if user is None:
        return await message.reply_text("<b>❌ User nahi mila.</b>")
    await message.reply_text(build_info(user))
