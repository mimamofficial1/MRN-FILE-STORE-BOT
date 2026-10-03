import asyncio
import logging
import logging.config
from datetime import date, datetime

import pytz
from aiohttp import web
from pyrogram import idle

# Get logging configurations
logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

from config import LOG_CHANNEL, PORT
from Script import script
from TechVJ.bot import StreamBot
from TechVJ.server import web_server
from TechVJ.utils.keepalive import ping_server
from TechVJ.utils.watchdog import run_watchdog, mark_ok
from plugins.dbusers import db
from plugins.settings_db import ensure_join_request_indexes


StreamBot.start()
loop = asyncio.get_event_loop()


async def start():
    print('\n')
    print('Initalizing Tech VJ Bot')
    me = await StreamBot.get_me()
    StreamBot.username = me.username
    mark_ok()
    try:
        await db.ensure_indexes()
        await ensure_join_request_indexes()
    except Exception as e:
        logging.error(f"Could not ensure MongoDB indexes: {e}")
    asyncio.create_task(run_watchdog(StreamBot))
    # NOTE: plugins are already loaded by Pyrogram itself (plugins={"root": "plugins"}
    # in TechVJ/bot/__init__.py) when StreamBot.start() runs above. The old manual
    # importlib loop re-executed every plugin a second time (duplicate Mongo clients).
    asyncio.create_task(ping_server())  # harmless self-ping, runs on every host (Railway/Koyeb/Heroku)

    # Start the web server (health check at "/") FIRST, before anything that
    # could fail (like messaging LOG_CHANNEL below). Railway's deploy health
    # check waits for "/" to respond - if the LOG_CHANNEL message crashed the
    # old code before the server ever came up, the health check would never
    # succeed and the platform could end up in a restart loop.
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()

    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz)
    today = now.date()  # IST date, not date.today()'s server/UTC date
    time = now.strftime("%I:%M:%S %p")  # 12-hour clock to match %p (AM/PM)
    try:
        await StreamBot.send_message(chat_id=LOG_CHANNEL, text=script.RESTART_TXT.format(today, time))
    except Exception as e:
        logging.error(f"Could not send restart notice to LOG_CHANNEL: {e}")

    print("Bot Started Successfully!")
    await idle()


if __name__ == '__main__':
    try:
        loop.run_until_complete(start())
    except KeyboardInterrupt:
        logging.info('Service Stopped Bye 👋')
