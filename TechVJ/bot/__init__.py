from pyrogram import Client, types
from config import *
from typing import Union, Optional, AsyncGenerator

class StreamXBot(Client):

    def __init__(self):
        super().__init__(
            name="vjfiletolink",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workers=50,
            plugins={"root": "plugins"},
            sleep_threshold=10,
        )
    async def iter_messages(
        self,
        chat_id: Union[int, str],
        limit: int,
        offset: int = 0,
    ) -> Optional[AsyncGenerator["types.Message", None]]:
        """Iterate through a chat sequentially.
        This convenience method does the same as repeatedly calling :meth:`~pyrogram.Client.get_messages` in a loop, thus saving
        you from the hassle of setting up boilerplate code. It is useful for getting the whole chat messages with a
        single call.
        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                
            limit (``int``):
                Identifier of the last message to be returned.
                
            offset (``int``, *optional*):
                Identifier of the first message to be returned.
                Defaults to 0.
        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.Message` objects.
        Example:
            .. code-block:: python
                for message in app.iter_messages("pyrogram", 1, 15000):
                    print(message.text)
        """
        current = offset
        while current <= limit:
            # at most 200 ids per request, both ends inclusive; progress is
            # driven by the ids we asked for (not by how many came back)
            ids = list(range(current, min(current + 199, limit) + 1))
            messages = await self.get_messages(chat_id, ids)
            for message in messages:
                yield message
            # Continue after the last message Telegram actually returned. If it
            # ever answers with fewer than the ids we asked for (a server-side
            # cap), nothing is skipped; if it returns all of them, same result
            # as before. max() guarantees forward progress.
            last_seen = messages[-1].id if messages else ids[-1]
            current = max(current + 1, last_seen + 1)
      
StreamBot = StreamXBot()
