import os
import time
import asyncio
import logging

logger = logging.getLogger(__name__)

# Updated every successful ping; the health route considers the bot
# "down" only if this hasn't been refreshed in a long while. This avoids
# treating Pyrogram's normal brief reconnects as an outage and triggering
# needless platform restarts (which was itself causing a restart loop).
_last_ok = time.time()

# How long the bot may go without a successful ping before we call it
# genuinely stuck. Keep this well above Pyrogram's normal reconnect time.
STALE_AFTER_SECONDS = 150

# The "/" health route turning unhealthy is NOT, by itself, enough to get the
# process restarted: Railway's healthcheckPath is only consulted during a
# deploy's rollout window, not as an ongoing monitor of an already-running
# container, and its restartPolicyType ("ON_FAILURE") only reacts to the
# process actually exiting with a non-zero code - never to a process that is
# merely hung but still "running". So once the bot has been stuck for this
# much longer than STALE_AFTER_SECONDS, the watchdog forces a real crash
# itself, which IS something the restart policy will catch and fix.
RESTART_AFTER_SECONDS = 300


def mark_ok():
    global _last_ok
    _last_ok = time.time()


def is_bot_healthy():
    return (time.time() - _last_ok) < STALE_AFTER_SECONDS


async def run_watchdog(bot):
    """Periodically confirms the bot can actually talk to Telegram, and
    force-exits the process if it's been stuck for too long so the
    hosting platform's restart policy actually kicks in."""
    while True:
        try:
            await asyncio.wait_for(bot.get_me(), timeout=15)
            mark_ok()
        except Exception as e:
            logger.warning(f"[WATCHDOG] ping failed: {e}")
            stuck_for = time.time() - _last_ok
            if stuck_for > RESTART_AFTER_SECONDS:
                logger.critical(
                    f"[WATCHDOG] bot unresponsive for {int(stuck_for)}s - forcing a restart"
                )
                os._exit(1)
        await asyncio.sleep(30)
