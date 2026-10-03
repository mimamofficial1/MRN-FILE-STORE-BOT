import os
import logging
import random
import asyncio
import time
from validators import domain
from Script import script
from plugins.dbusers import db
from pyrogram import Client, filters, enums
from plugins.users_api import get_user, update_user_info
from pyrogram.errors import ChatAdminRequired, FloodWait, PeerIdInvalid, ChannelInvalid
import html
from pyrogram.types import *
from utils import verify_user, check_token, check_verification, get_token
from plugins.settings_db import get_settings
from plugins.force_sub import not_joined_channels, force_sub_join_buttons, get_missing_and_buttons
from TechVJ.server.streamer import build_stream_urls
from config import *
import re
import json
import base64
logger = logging.getLogger(__name__)

BATCH_FILES = {}


def get_size(size):
    """Get size in readable format"""

    units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"]
    size = float(size)
    i = 0
    while size >= 1024.0 and i < len(units):
        i += 1
        size /= 1024.0
    return "%.2f %s" % (size, units[i])

def formate_file_name(file_name):
    if not file_name:
        return file_name
    chars = ["[", "]", "(", ")"]
    for c in chars:
        file_name = file_name.replace(c, "")  # was discarding the result before - a no-op bug
    file_name = ' '.join(filter(lambda x: not x.startswith('http') and not x.startswith('@') and not x.startswith('www.'), file_name.split()))
    return file_name


def stream_download_buttons(chat_id, message_id, media):
    """Fast Download / Watch Online button row for a delivered file.
    Built from data already in hand (no extra Telegram API call). Returns
    [] if STREAM_MODE is off or the media has no usable file info."""
    if not STREAM_MODE:
        return []
    try:
        download_url, watch_url = build_stream_urls(
            chat_id, message_id,
            getattr(media, "file_name", "") or "file",
        )
        return [[
            InlineKeyboardButton('🚀 ꜰᴀꜱᴛ ᴅᴏᴡɴʟᴏᴀᴅ 🚀', url=download_url),
            InlineKeyboardButton('🖥 ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ 🖥', url=watch_url),
        ]]
    except Exception:
        logger.error("Failed to build stream/download buttons", exc_info=True)
        return []


async def _forward_accessed_to_log(client, refs):
    """Forward the actual files/videos a user just accessed (batch) into
    LOG_CHANNEL, grouped and chunked, so admins can see/open the real files
    instead of only reading their names. Runs as a background task."""
    by_channel = {}
    for ch, mid in refs:
        by_channel.setdefault(ch, []).append(mid)
    for ch, ids in by_channel.items():
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            try:
                await client.forward_messages(LOG_CHANNEL, ch, chunk)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    await client.forward_messages(LOG_CHANNEL, ch, chunk)
                except Exception:
                    pass
            except Exception:
                pass
            await asyncio.sleep(0.5)


async def _forward_single_to_log(client, msg):
    """Forward the single file/video a user just accessed into LOG_CHANNEL
    so admins can see the real file, not just its name. Background task."""
    try:
        await client.forward_messages(LOG_CHANNEL, msg.chat.id, msg.id)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try:
            await client.forward_messages(LOG_CHANNEL, msg.chat.id, msg.id)
        except Exception:
            pass
    except Exception:
        pass


# Keep references to background tasks so they can't be garbage-collected mid-run.
_BG_TASKS = set()


def _spawn(coro):
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


# Pending auto-deletes are stored in MongoDB so they survive a restart/redeploy.
# (Before, the timer lived only in memory: restart within the delay window =
# the delivered files were never deleted.)
_pending_col = db.db.pending_deletes
_MAX_PENDING_AGE = 2 * 86400  # bots can't delete messages older than ~48h anyway


async def _schedule_delete(client, chat_id, msg_ids, notice, delay, final_text):
    """Save the pending delete in MongoDB and start its timer. Never raises -
    it runs after the file is already delivered and must not break /start."""
    try:
        msg_ids = [int(i) for i in msg_ids]
        delete_at = time.time() + delay
        access_hash = None
        try:
            peer = await client.resolve_peer(chat_id)
            access_hash = getattr(peer, "access_hash", None)
        except Exception:
            pass
        pid = None
        try:
            res = await _pending_col.insert_one({
                "chat_id": int(chat_id),
                "access_hash": access_hash,
                "msg_ids": msg_ids,
                "notice_id": int(notice.id),
                "final_text": final_text,
                "delete_at": delete_at,
            })
            pid = res.inserted_id
        except Exception as e:
            logger.error(f"Could not save pending delete (timer still runs in memory): {e}")
        _spawn(_run_delete(client, pid, int(chat_id), msg_ids, int(notice.id), delete_at, final_text))
    except Exception:
        logger.error("Failed to schedule auto-delete", exc_info=True)


async def _run_delete(client, pid, chat_id, msg_ids, notice_id, delete_at, final_text):
    wait = delete_at - time.time()
    if wait > 0:
        await asyncio.sleep(wait)
    failed = False
    for i in range(0, len(msg_ids), 100):
        chunk = msg_ids[i:i + 100]
        try:
            await client.delete_messages(chat_id, chunk)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await client.delete_messages(chat_id, chunk)
            except Exception:
                failed = True
        except Exception as e:
            failed = True
            logger.error(f"Auto-delete failed for chat {chat_id}: {e}")
    try:
        await client.edit_message_text(chat_id, notice_id, final_text)
    except Exception:
        pass
    if pid is not None and not failed:
        try:
            await _pending_col.delete_one({"_id": pid})
        except Exception:
            pass


async def restore_pending_deletes(client):
    """Called once from bot.py after startup: re-arm every auto-delete that was
    still waiting when the bot last stopped (overdue ones run immediately)."""
    try:
        docs = await _pending_col.find({}).to_list(length=None)
    except Exception as e:
        logger.error(f"Could not load pending deletes: {e}")
        return 0
    now = time.time()
    restored = 0
    for d in docs:
        try:
            if now - d["delete_at"] > _MAX_PENDING_AGE:
                await _pending_col.delete_one({"_id": d["_id"]})
                continue
            if d.get("access_hash"):
                # the peer cache is empty after a restart - without this the user's
                # chat can't be resolved and the delete would fail
                try:
                    await client.storage.update_peers([(d["chat_id"], d["access_hash"], "user", None, None)])
                except Exception:
                    pass
            _spawn(_run_delete(client, d["_id"], d["chat_id"], d["msg_ids"], d["notice_id"], d["delete_at"], d["final_text"]))
            restored += 1
        except Exception:
            logger.error("Bad pending-delete record skipped", exc_info=True)
    return restored


def _build_custom_buttons(settings):
    """Rows of valid custom buttons from /settings. Anything with an empty text
    or a non-link url is skipped - ONE bad button used to make Telegram reject
    every single delivery (BUTTON_URL_INVALID) -> 'file could not be found'."""
    rows = []
    for row in (settings.get("custom_buttons") or []):
        btns = []
        for b in row:
            try:
                text = (b.get("text") or "").strip()
                url = (b.get("url") or "").strip()
            except AttributeError:
                continue
            if text and url.startswith(("http://", "https://", "tg://")):
                btns.append(InlineKeyboardButton(text, url=url))
        if btns:
            rows.append(btns)
    return rows


def _markup(rows):
    # never send an empty InlineKeyboardMarkup([]) - use no markup at all
    return InlineKeyboardMarkup(rows) if rows else None


async def _safe_copy(src, chat_id, caption=None, rows=None, protect=False, pacer=None):
    """Copy `src` to `chat_id`. If the fancy version (caption + buttons) is
    rejected by Telegram, retry with less and less until the user at least
    gets the file. Returns (sent_message, first_error_or_None)."""
    attempts = [
        {"caption": caption, "reply_markup": _markup(rows), "protect_content": protect},
        {"caption": caption, "protect_content": protect},
        {"protect_content": protect},
        {},
    ]
    first_error = None
    last_error = None
    for kw in attempts:
        kw = {k: v for k, v in kw.items() if not (k == "caption" and v is None)}
        for _ in range(2):  # 2nd pass only used after a FloodWait
            try:
                t0 = time.monotonic()
                sent = await src.copy(chat_id=chat_id, **kw)
                if pacer is not None and time.monotonic() - t0 > 1.5:
                    # Pyrogram silently sleeps through short FloodWaits, so a slow
                    # copy = Telegram is throttling us -> slow the batch pace down
                    pacer["delay"] = min(pacer["delay"] * 2, 1.0)
                return sent, first_error
            except FloodWait as e:
                if pacer is not None:
                    pacer["delay"] = min(pacer["delay"] * 2, 1.0)
                await asyncio.sleep(e.value)
                continue
            except Exception as e:
                last_error = e
                if first_error is None:
                    first_error = e
                break
    raise last_error


async def _get_log_message(client, mid):
    """Fetch a stored message from LOG_CHANNEL. Re-resolves the channel if the
    bot's peer cache is empty (happens after a redeploy) and fails loudly if
    the message is gone, instead of returning an empty Message."""
    try:
        msg = await client.get_messages(LOG_CHANNEL, mid)
    except (PeerIdInvalid, ChannelInvalid):
        await client.get_chat(LOG_CHANNEL)
        msg = await client.get_messages(LOG_CHANNEL, mid)
    if not msg or msg.empty:
        raise ValueError(f"Message {mid} not found in LOG_CHANNEL (deleted, or wrong LOG_CHANNEL)")
    return msg


async def _log_warning(client, text):
    try:
        await client.send_message(LOG_CHANNEL, text)
    except Exception:
        pass


@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    username = client.me.username
    user_doc = await db.get_user(message.from_user.id)
    if not user_doc:
        await db.add_user(message.from_user.id, message.from_user.first_name)
        await client.send_message(LOG_CHANNEL, script.LOG_TEXT.format(message.from_user.id, message.from_user.mention))
    elif user_doc.get('banned', False):
        return await message.reply_text("<b>🚫 You are banned from using this bot.</b>")

    settings = await get_settings()
    missing_channels, fsub_buttons = await get_missing_and_buttons(client, message.from_user.id, settings)
    if missing_channels:
        buttons = fsub_buttons
        param = message.command[1] if len(message.command) == 2 else "-"
        buttons.append([InlineKeyboardButton("🔄 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"fsub_verify:{param}")])
        fsub_text = settings.get("force_sub_message") or "<b>Please join our channel(s) to use this bot.</b>"
        fsub_photo = settings.get("force_sub_photo")
        if fsub_photo:
            try:
                return await message.reply_photo(
                    fsub_photo,
                    caption=fsub_text,
                    reply_markup=InlineKeyboardMarkup(buttons),
                )
            except Exception:
                pass  # bad/expired file_id -> fall back to text-only below
        return await message.reply_text(
            fsub_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True
        )

    async def send_main_menu():
        """The default /start greeting (menu + custom/default welcome photo).
        Used for a plain /start, and as a safe fallback whenever a start
        parameter turns out to not be a real, decodable file link."""
        buttons = [[
            InlineKeyboardButton('👨‍💻 ᴍᴀɪɴᴛᴀɪɴᴇʀ', url='https://t.me/Mimam_Officialx'),
            InlineKeyboardButton('📢 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ', url='https://t.me/Mrn_Officialx')
            ],[
            InlineKeyboardButton('💁‍♂ ʜᴇʟᴘ', callback_data='help'),
            InlineKeyboardButton('💎 ᴀʙᴏᴜᴛ', callback_data='about')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        me = client.me
        start_caption = settings.get("start_message") or script.START_TXT
        try:
            start_caption = start_caption.format(message.from_user.mention, me.mention)
        except (IndexError, KeyError):
            pass
        start_photo = settings.get("start_photo") or random.choice(PICS)
        try:
            await message.reply_photo(
                photo=start_photo,
                caption=start_caption,
                reply_markup=reply_markup
            )
        except Exception:
            # bad/expired custom file_id -> fall back to a default random pic
            await message.reply_photo(
                photo=random.choice(PICS),
                caption=start_caption,
                reply_markup=reply_markup
            )

    if len(message.command) != 2:
        await send_main_menu()
        return

    
    data = message.command[1]
    try:
        pre, file_id = data.split('_', 1)
    except:
        file_id = data
        pre = ""
    if data.split("-", 1)[0] == "verify":
        userid = data.split("-", 2)[1]
        token = data.split("-", 3)[2]
        if str(message.from_user.id) != str(userid):
            return await message.reply_text(
                text="<b>Invalid link or Expired link !</b>",
                protect_content=True
            )
        is_valid = await check_token(client, userid, token)
        if is_valid == True:
            await message.reply_text(
                text=f"<b>Hey {message.from_user.mention}, You are successfully verified !\nNow you have unlimited access for all files till today midnight.</b>",
                protect_content=True
            )
            await verify_user(client, userid, token)
            return
        else:
            return await message.reply_text(
                text="<b>Invalid link or Expired link !</b>",
                protect_content=True
            )
    elif data.split("-", 1)[0] == "BATCH":
        try:
            if VERIFY_MODE == True and not await check_verification(client, message.from_user.id):
                btn = [[
                    InlineKeyboardButton("Verify", url=await get_token(client, message.from_user.id, f"https://telegram.me/{username}?start="))
                ],[
                    InlineKeyboardButton("How To Open Link & Verify", url=VERIFY_TUTORIAL)
                ]]
                await message.reply_text(
                    text="<b>You are not verified !\nKindly verify to continue !</b>",
                    protect_content=True,
                    reply_markup=InlineKeyboardMarkup(btn)
                )
                return
        except Exception as e:
            return await message.reply_text(f"**Error - {e}**")
        sts = await message.reply("**🔺 ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ**")
        file_id = data.split("-", 1)[1]
        msgs = BATCH_FILES.get(file_id)
        if not msgs:
            decode_file_id = base64.urlsafe_b64decode(file_id + "=" * (-len(file_id) % 4)).decode("ascii")
            msg = await client.get_messages(LOG_CHANNEL, int(decode_file_id))
            media = getattr(msg, msg.media.value)
            file = await client.download_media(media.file_id)  # keep file_id intact - it is the BATCH_FILES cache key
            try: 
                with open(file) as file_data:
                    msgs=json.loads(file_data.read())
            except:
                try:
                    os.remove(file)
                except Exception:
                    pass
                await sts.edit("FAILED")
                return await client.send_message(LOG_CHANNEL, "UNABLE TO OPEN FILE.")
            os.remove(file)
            BATCH_FILES[file_id] = msgs
            
        filesarr = []
        titles = []
        accessed_refs = []  # (channel_id, msg_id) pairs actually delivered - used to
                             # forward the real files/videos into the access log below.

        # Fetch every source message in bulk (chunks of 200) instead of one
        # get_messages() call per file - this is the single biggest speedup
        # for large batches since each call previously cost a network round trip.
        by_channel = {}
        order = []
        for m in msgs:
            ch = int(m.get("channel_id"))
            mid = m.get("msg_id")
            by_channel.setdefault(ch, []).append(mid)
            order.append((ch, mid))

        fetched = {}
        for ch, ids in by_channel.items():
            for i in range(0, len(ids), 200):
                chunk = ids[i:i + 200]
                try:
                    results = await client.get_messages(ch, chunk)
                except Exception:
                    results = []
                for r in results or []:
                    if r:
                        fetched[(ch, r.id)] = r

        pacer = {"delay": 0.25}  # starts fast; doubles (max 1s) whenever Telegram throttles
        for channel_id, msgid in order:
            info = fetched.get((channel_id, msgid))
            if not info:
                continue
            if info.media:
                file_type = info.media
                file = getattr(info, file_type.value)
                f_caption = getattr(info, 'caption', '')
                if f_caption:
                    f_caption = f_caption.html
                old_title = getattr(file, "file_name", "")
                title = formate_file_name(old_title)
                size=get_size(int(file.file_size))
                batch_caption = settings.get("custom_caption") or BATCH_FILE_CAPTION
                if batch_caption:
                    try:
                        f_caption=batch_caption.format(file_name= '' if title is None else title, file_size='' if size is None else size, file_caption='' if f_caption is None else f_caption)
                    except:
                        f_caption=f_caption
                if f_caption is None:
                    f_caption = f"{title}"
                button = _build_custom_buttons(settings)
                button.extend(stream_download_buttons(channel_id, msgid, file))
                protect = settings.get("protect_content", False)
                try:
                    msg, _err = await _safe_copy(info, message.from_user.id, caption=f_caption, rows=button, protect=protect, pacer=pacer)
                except Exception:
                    continue
            else:
                title = "Unknown File"
                title = "Unknown File"
                protect = settings.get("protect_content", False)
                button = _build_custom_buttons(settings)
                try:
                    msg, _err = await _safe_copy(info, message.from_user.id, rows=button, protect=protect, pacer=pacer)
                except Exception:
                    continue
            filesarr.append(msg)
            titles.append(title)
            accessed_refs.append((channel_id, msgid))
            await asyncio.sleep(pacer["delay"])
        try:
            if titles:
                shown = titles[:30]
                files_list = "\n".join(f"{i+1}. <code>{t}</code>" for i, t in enumerate(shown))
                if len(titles) > 30:
                    files_list += f"\n... and {len(titles) - 30} more"
            else:
                files_list = "N/A"
            await client.send_message(
                LOG_CHANNEL,
                f"<b>📥 #FileAccess (Batch)</b>\n\n"
                f"👤 User: {message.from_user.mention} (<code>{message.from_user.id}</code>)\n"
                f"📦 Files Accessed: <code>{len(filesarr)}</code>\n\n"
                f"{files_list}\n\n"
                f"⬇️ ᴀᴄᴛᴜᴀʟ ꜰɪʟᴇꜱ ʙᴇʟᴏᴡ"
            )
            # Also forward the real accessed files/videos into the log channel
            # (not just their names) - runs in the background so it never
            # slows down the reply the user is waiting for.
            asyncio.create_task(_forward_accessed_to_log(client, accessed_refs))
        except:
            pass
        await sts.delete()
        if settings.get("auto_delete", True):
            del_minutes = max(1, settings.get("auto_delete_time", 1800) // 60)
            k = await client.send_message(chat_id = message.from_user.id, text=(
                f"🔔 <b><u>Just a Quick Reminder !!</u>\n\n"
                f"⏳ This Content Will Be Automatically Deleted After <u>{del_minutes} Minutes</u> Due to Copyright Issues.\n\n"
                f"🔗 Once Deleted, You'll Need to Open the Link Again to Access It.\n\n"
                f"📌 Want to Keep It? Save or Forward It to your Saved Messages Before It Expires. 👍\n\n"
                f"❤️ Save It Now, Enjoy It Later! 💫\n\n"
                f"👨‍💻 Powered By @Mrn_Officialx ⚡\n\n"
                f"🧑‍🔧 Help &amp; Support &gt;&gt;&gt; @Mrn_Contact_Bot</b>"
            ))
            await _schedule_delete(client, message.from_user.id, [x.id for x in filesarr], k,
                                   settings.get("auto_delete_time", 1800),
                                   "<b>Your All Files/Videos is successfully deleted!!!</b>")
        return


    try:
        pre, decode_file_id = ((base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))).decode("ascii")).split("_", 1)
    except Exception:
        # Not a real file link (e.g. the "?start=true" Continue button used after
        # force-sub when there was no original deep-link param) - just greet them
        # instead of crashing silently with no reply at all.
        await send_main_menu()
        return
    if VERIFY_MODE == True and not await check_verification(client, message.from_user.id):
        btn = [[
            InlineKeyboardButton("Verify", url=await get_token(client, message.from_user.id, f"https://telegram.me/{username}?start="))
        ],[
            InlineKeyboardButton("How To Open Link & Verify", url=VERIFY_TUTORIAL)
        ]]
        await message.reply_text(
            text="<b>You are not verified !\nKindly verify to continue !</b>",
            protect_content=True,
            reply_markup=InlineKeyboardMarkup(btn)
        )
        return
    try:
        msg = await _get_log_message(client, int(decode_file_id))
        real_file_types = ("document", "video", "audio", "photo", "animation", "voice", "video_note")
        protect = settings.get("protect_content", False)
        rows = _build_custom_buttons(settings)
        if msg.media and msg.media.value in real_file_types:
            media = getattr(msg, msg.media.value)
            orig_caption = getattr(msg, 'caption', None)
            orig_caption = orig_caption.html if orig_caption else None
            title = orig_caption or formate_file_name(getattr(media, "file_name", "")) or "File"
            size = get_size(getattr(media, "file_size", 0) or 0)
            f_caption = f"<code>{title}</code>"
            single_caption = settings.get("custom_caption") or CUSTOM_FILE_CAPTION
            if single_caption:
                try:
                    f_caption = single_caption.format(file_name=title, file_size=size, file_caption=title)
                except Exception:
                    f_caption = f"<code>{title}</code>"
            rows.extend(stream_download_buttons(msg.chat.id, msg.id, media))
            del_msg, copy_err = await _safe_copy(msg, message.from_user.id, caption=f_caption, rows=rows, protect=protect)
        else:
            # text / link / sticker / anything else: send as-is
            title = "Unknown File"
            del_msg, copy_err = await _safe_copy(msg, message.from_user.id, rows=rows, protect=protect)
        if copy_err:
            # file WAS delivered, but only after dropping caption/buttons -> tell admins why
            logger.error(f"Delivered with fallback, first error: {copy_err}")
            _spawn(_log_warning(
                client,
                f"<b>⚠️ #DeliveryFallback</b>\n\n🆔 Message ID: <code>{decode_file_id}</code>\n"
                f"❌ First error: <code>{html.escape(str(copy_err))[:500]}</code>\n\n"
                f"Check /settings → custom buttons / caption."
            ))
        try:
            await client.send_message(
                LOG_CHANNEL,
                f"<b>📥 #FileAccess</b>\n\n"
                f"👤 User: {message.from_user.mention} (<code>{message.from_user.id}</code>)\n"
                f"🎬 File: <code>{title}</code>"
            )
            # Also forward the real accessed file into the log channel (not
            # just its name), in the background so delivery isn't delayed.
            asyncio.create_task(_forward_single_to_log(client, msg))
        except Exception:
            pass
        if settings.get("auto_delete", True):
            del_minutes = max(1, settings.get("auto_delete_time", 1800) // 60)
            k = await client.send_message(chat_id = message.from_user.id, text=(
                f"🔔 <b><u>Just a Quick Reminder !!</u>\n\n"
                f"⏳ This Content Will Be Automatically Deleted After <u>{del_minutes} Minutes</u> Due to Copyright Issues.\n\n"
                f"🔗 Once Deleted, You'll Need to Open the Link Again to Access It.\n\n"
                f"📌 Want to Keep It? Save or Forward It to your Saved Messages Before It Expires. 👍\n\n"
                f"❤️ Save It Now, Enjoy It Later! 💫\n\n"
                f"👨‍💻 Powered By @Mrn_Officialx ⚡\n\n"
                f"🧑‍🔧 Help &amp; Support &gt;&gt;&gt; @Mrn_Contact_Bot</b>"
            ))
            await _schedule_delete(client, message.from_user.id, [del_msg.id], k,
                                   settings.get("auto_delete_time", 1800),
                                   "<b>Your File/Video is successfully deleted!!!</b>")
        return
    except Exception as e:
        logger.error(f"Failed to deliver file for decode_file_id={decode_file_id}: {e}", exc_info=True)
        try:
            await client.send_message(
                LOG_CHANNEL,
                f"<b>⚠️ #FileDeliveryFailed</b>\n\n"
                f"👤 User: {message.from_user.mention} (<code>{message.from_user.id}</code>)\n"
                f"🆔 Message ID tried: <code>{decode_file_id}</code>\n"
                f"❌ Error: <code>{html.escape(type(e).__name__ + ': ' + str(e))[:700]}</code>"
            )
        except Exception:
            pass
        reason = ""
        if message.from_user.id in ADMINS:  # only admins see the technical reason
            reason = f"\n\n<code>{html.escape(type(e).__name__ + ': ' + str(e))[:500]}</code>"
        await message.reply_text(
            "<b>❌ This file could not be found or delivered. It may have been removed, or the link is broken. "
            "Please ask the admin to generate a new link.</b>" + reason
        )
        

@Client.on_message(filters.command('api') & filters.private)
async def shortener_api_handler(client, m: Message):
    user_id = m.from_user.id
    user = await get_user(user_id)
    cmd = m.command

    if len(cmd) == 1:
        s = script.SHORTENER_API_MESSAGE.format(base_site=user["base_site"], shortener_api=user["shortener_api"])
        return await m.reply(s)

    elif len(cmd) == 2:    
        api = cmd[1].strip()
        if api.lower() == "none":
            # "/api None" removes the saved API (it used to be stored as the literal string "None")
            await update_user_info(user_id, {"shortener_api": None})
            return await m.reply("<b>Shortener API removed successfully</b>")
        await update_user_info(user_id, {"shortener_api": api})
        await m.reply("<b>Shortener API updated successfully to</b> " + api)


@Client.on_message(filters.command("base_site") & filters.private)
async def base_site_handler(client, m: Message):
    user_id = m.from_user.id
    user = await get_user(user_id)
    cmd = m.command
    text = f"`/base_site (base_site)`\n\n<b>Current base site: None\n\n EX:</b> `/base_site shortnerdomain.com`\n\nIf You Want To Remove Base Site Then Copy This And Send To Bot - `/base_site None`"
    if len(cmd) == 1:
        return await m.reply(text=text, disable_web_page_preview=True)
    elif len(cmd) == 2:
        base_site = cmd[1].strip()
        if base_site.lower() == "none":
            # "/base_site None" removes the saved base site (the old `== None` check never matched a string)
            await update_user_info(user_id, {"base_site": None})
            return await m.reply("<b>Base Site removed successfully</b>")
            
        if not domain(base_site):
            return await m.reply(text=text, disable_web_page_preview=True)
        await update_user_info(user_id, {"base_site": base_site})
        await m.reply("<b>Base Site updated successfully</b>")


@Client.on_callback_query(filters.regex(r"^(close_data|about|start|help)$"))
async def cb_handler(client: Client, query: CallbackQuery):
    if query.data == "close_data":
        await query.message.delete()
    elif query.data == "about":
        buttons = [[
            InlineKeyboardButton('Hᴏᴍᴇ', callback_data='start'),
            InlineKeyboardButton('🔒 Cʟᴏsᴇ', callback_data='close_data')
        ]]
        await client.edit_message_media(
            query.message.chat.id, 
            query.message.id, 
            InputMediaPhoto(random.choice(PICS))
        )
        reply_markup = InlineKeyboardMarkup(buttons)
        me2 = (await client.get_me()).mention
        await query.message.edit_text(
            text=script.ABOUT_TXT.format(me2),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )

    
    elif query.data == "start":
        buttons = [[
            InlineKeyboardButton('👨‍💻 ᴍᴀɪɴᴛᴀɪɴᴇʀ', url='https://t.me/Mimam_Officialx'),
            InlineKeyboardButton('📢 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ', url='https://t.me/Mrn_Officialx')
            ],[
            InlineKeyboardButton('💁‍♂ ʜᴇʟᴘ', callback_data='help'),
            InlineKeyboardButton('💎 ᴀʙᴏᴜᴛ', callback_data='about')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        settings = await get_settings()
        me2 = (await client.get_me()).mention
        start_caption = settings.get("start_message") or script.START_TXT
        try:
            start_caption = start_caption.format(query.from_user.mention, me2)
        except (IndexError, KeyError):
            pass
        start_photo = settings.get("start_photo") or random.choice(PICS)
        try:
            await client.edit_message_media(
                query.message.chat.id,
                query.message.id,
                InputMediaPhoto(start_photo, caption=start_caption, parse_mode=enums.ParseMode.HTML),
            )
            await query.message.edit_reply_markup(reply_markup=reply_markup)
        except Exception:
            # bad/expired custom start_photo file_id -> fall back to default pic
            await client.edit_message_media(
                query.message.chat.id,
                query.message.id,
                InputMediaPhoto(random.choice(PICS), caption=start_caption, parse_mode=enums.ParseMode.HTML),
            )
            await query.message.edit_reply_markup(reply_markup=reply_markup)

    
    elif query.data == "help":
        buttons = [[
            InlineKeyboardButton('Hᴏᴍᴇ', callback_data='start'),
            InlineKeyboardButton('🔒 Cʟᴏsᴇ', callback_data='close_data')
        ]]
        await client.edit_message_media(
            query.message.chat.id, 
            query.message.id, 
            InputMediaPhoto(random.choice(PICS))
        )
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.HELP_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
