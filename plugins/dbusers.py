import time
import motor.motor_asyncio
from config import DB_NAME, DB_URI

_USER_TTL = 60


class Database:
    
    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        )
        self.db = self._client[database_name]
        self.col = self.db.users
        # /start (ban check) and every link generation look the user up. Kept in
        # memory for _USER_TTL seconds; ban/unban/add/delete clear it at once.
        self._ucache = {}

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
        )
    
    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)
        self._ucache.pop(int(id), None)
    
    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)

    async def get_user(self, id):
        """Single query combining what is_user_exist + is_user_banned used
        to do as two separate round trips. Cached for _USER_TTL seconds."""
        id = int(id)
        now = time.monotonic()
        hit = self._ucache.get(id)
        if hit is not None and now - hit[1] < _USER_TTL:
            return dict(hit[0])
        user = await self.col.find_one({'id': id})
        if user is not None:            # "not found" is never cached: a new user must be created at once
            if len(self._ucache) > 30000:
                self._ucache.clear()
            self._ucache[id] = (user, now)
            return dict(user)
        return None

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count
    
    async def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        self._ucache.pop(int(user_id), None)
        await self.col.delete_many({'id': int(user_id)})
        self._ucache.pop(int(user_id), None)

    async def ban_user(self, user_id):
        self._ucache.pop(int(user_id), None)
        await self.col.update_one({'id': int(user_id)}, {'$set': {'banned': True}})
        self._ucache.pop(int(user_id), None)

    async def unban_user(self, user_id):
        self._ucache.pop(int(user_id), None)
        await self.col.update_one({'id': int(user_id)}, {'$set': {'banned': False}})
        self._ucache.pop(int(user_id), None)

    async def is_user_banned(self, user_id):
        user = await self.get_user(user_id)
        if user:
            return user.get('banned', False)
        return False

    async def total_banned_count(self):
        count = await self.col.count_documents({'banned': True})
        return count

    async def ensure_indexes(self):
        """Every /start, every genlink ban-check etc. looks a user up by
        'id' - without an index each of those does a full collection scan,
        getting slower as the user base grows. Non-unique on purpose: if any
        duplicate/mixed-type 'id' values already exist in production, a
        unique index would fail to build and could crash startup."""
        await self.col.create_index("id")


db = Database(DB_URI, DB_NAME)
