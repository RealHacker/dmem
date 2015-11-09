from dbase import *
import random, string
import contextlib
from utils import *
from redisstr import RedisStr

DELETE_PLACE_HOLDER = "__TO_BE_DELETED__"

FIND_ITEM_LUA_SCRIPT= """
local key = KEYS[1]
local obj = ARGV[1]
local tkey = KEYS[2]
local tobj = ARGV[2]
local items = redis.call('lrange', key, 0, -1)
local types = redis.call('lrange', tkey, 0, -1)
for i = 1, #items do
    if items[i] == obj and types[i] == tobj then
        return i - 1
    end
end 
return -1
"""

REVERSE_LUA_SCRIPT = """
local key = KEYS[1]
local tkey = KEYS[2]
local llen = redis.call('llen', key)
local half = math.floor(llen/2)
for i = 1, half do
    local obj1 = redis.call('lindex', key, i-1)
    local obj2 = redis.call('lindex', key, llen-i)
    redis.call('lset', key, i-1, obj2)
    redis.call('lset', key, llen-i, obj1)
    obj1 = redis.call('lindex', tkey, i-1)
    obj2 = redis.call('lindex', tkey, llen-i)
    redis.call('lset', tkey, i-1, obj2)
    redis.call('lset', tkey, llen-i, obj1)
end
"""

INSERT_LUA_SCRIPT = """
local key = KEYS[1]
local tkey = KEYS[2]
local idx = tonumber(ARGV[1])
local obj = ARGV[2]
local t = ARGV[3]

local llen = redis.call('llen', key)
if idx >= llen then 
    redis.call('rpush', key, obj)
    redis.call('rpush', tkey, t)
else
    local val = redis.call('lindex', key, idx)
    local valt = redis.call('lindex', tkey, idx)
    redis.call('lset', key, idx, '_REDIS_LUA_PLACEHOLDER_')
    redis.call('lset', tkey, idx, '_REDIS_LUA_PLACEHOLDER_')
    redis.call('linsert', key, 'BEFORE', '_REDIS_LUA_PLACEHOLDER_', obj)
    redis.call('linsert', tkey, 'BEFORE', '_REDIS_LUA_PLACEHOLDER_', t)
    redis.call('lset', key, idx+1, val)
    redis.call('lset', tkey, idx+1, valt)
end
"""

COUNT_LUA_SCRIPT = """
local key = KEYS[1]
local tkey = KEYS[2]
local target = ARGV[1]
local t = ARGV[2]
local items = redis.call('lrange', key, 0, -1)
local types = redis.call('lrange', tkey, 0, -1)
local cnt = 0
for i = 1, #items do
    if items[i] == target and types[i] == t then
        cnt = cnt+1
    end
end 
return cnt
"""

DELSLICE_LUA_SCRIPT = """
local key = KEYS[1]
local tkey = KEYS[2]
local start = tonumber(ARGV[1])
local stop = tonumber(ARGV[2])

local llen = redis.call('llen', key)
if stop > llen then
    stop = llen
end
for i = start, stop-1 do
    redis.call('lset', key, i, '_REDIS_LUA_PLACEHOLDER_')
    redis.call('lset', tkey, i, '_REDIS_LUA_PLACEHOLDER_')
end
redis.call('lrem', key, stop-start, '_REDIS_LUA_PLACEHOLDER_')
redis.call('lrem', tkey, stop-start, '_REDIS_LUA_PLACEHOLDER_')
"""

class RedisList(dbase):
    def __init__(self, _list=None):
        """
        >>> l = RedisList([1,2.0,True,"abc"])
        >>> l._load()
        [1, 2.0, True, 'abc']
        >>> l._load_objects_and_types()
        (['1', '2.0', '1', 'abc'], ['int', 'float', 'bool', 'str'])
        """
        dbase.__init__(self)
        # save to redis when initializing
        if _list:
            self.extend(_list)
        
    def initialize(self):
        self._type_ = "dmem:list"
        self._type_addr_ = "_type_" + self._addr_
        self.cache = None

    def _load_objects_and_types(self):
        objects = self.client.lrange(self._addr_, 0, -1)
        types = self.client.lrange(self._type_addr_, 0, -1)
        return objects, types

    def _load(self):
        objs, types = self._load_objects_and_types()
        assert(len(objs) == len(types))
        values = []
        for i in range(len(objs)):
            value = get_value_from_object_and_type(objs[i], types[i])
            values.append(value)
        return values

    def destroy(self):
        dbase.destroy(self)
        self.client.delete(self._type_addr_)
        
    @contextlib.contextmanager
    def loaded(self):
        self.cache = self._load()
        try: 
            yield self.cache
        finally:
            self.cache = None
    
    def __iter__(self):
	if self.cache:
		return iter(self.cache)
	return iter(self._load())

    def __len__(self):
        """
        >>> l = RedisList([1,2.0,True,"abc"])
        >>> len(l)
        4
        >>> with l.loaded():
        ...     print len(l)
        ...
        4
        """
        if self.cache:
            return len(self.cache)
        else:
            return self.client.llen(self._addr_)

    def __getitem__(self, idx):
        """
        >>> l = RedisList([1, 2.0, True, "abc", RedisStr("xyz")])
        >>> l[0]
        1
        >>> l[1]
        2.0
        >>> l[2]
        True
        >>> l[3]
        'abc'
        >>> l[4].getvalue()
        'xyz'
        >>> l[5]
        ...
        IndexError: Index out of range
        """
        if self.cache:
            return self.cache[idx]
        with self.client.pipeline() as pipe:
            pipe.lindex(self._addr_, idx)
            pipe.lindex(self._type_addr_, idx)
            [obj, t] = pipe.execute()
        if not obj:
            raise IndexError("Index out of range")
        v = get_value_from_object_and_type(obj, t)
        return v

    def __setitem__(self, idx, val):
        """
        >>> l = RedisList([1,2.0,True,"abc", RedisStr("xyz")])
        >>> l[0]=100
        >>> l[0]
        100
        >>> l[2]=False
        >>> l[2]
        False
        >>> l[4]=RedisStr("opq")
        >>> l[4].getvalue()
        'opq'
        >>> l[0]+=10
        >>> l[0]
        110
        """
        if self.cache:
            self.cache[idx] = val
        obj, t = get_redis_object_and_type(val)
        with self.client.pipeline() as pipe:
            pipe.lset(self._addr_, idx, obj)
            pipe.lset(self._type_addr_, idx, t)
            pipe.execute()

    def __delitem__(self, idx):
        """
        >>> l = RedisList([1,2.0,True,"abc", RedisStr("xyz")])
        >>> del l[1]
        >>> len(l)
        4
        >>> l._load()
        [1, True, 'abc', <__main__.RedisStr object at 0x01C79B30>]
        >>> del l[3]
        >>> l._load()
        [1, True, 'abc']
        """
        if self.cache:
            del self.cache[idx]
        with self.client.pipeline() as pipe:
            pipe.lset(self._addr_, idx, DELETE_PLACE_HOLDER)
            pipe.lrem(self._addr_, 1, DELETE_PLACE_HOLDER)
            pipe.lset(self._type_addr_, idx, DELETE_PLACE_HOLDER)
            pipe.lrem(self._type_addr_, 1, DELETE_PLACE_HOLDER)
            pipe.execute()

    def __getslice__(self, start, end):
        """
        >>> l = RedisList([110, 2.0, False, "abc", RedisStr("xyz")])
        >>> l[0:-1]
        [110, 2.0, False, 'abc']
        >>> l[-2:5]
        ['abc', <__main__.RedisStr object at 0x01C06BB0>]
        """
        if self.cache:
            return self.cache[start:end]
        if end==0:
            return []
        objs = self.client.lrange(self._addr_, start, end-1)
        ts = self.client.lrange(self._type_addr_, start, end-1)
        values = []
        for obj, t in zip(objs, ts):
            values.append(get_value_from_object_and_type(obj, t))
        return values

    def __delslice__(self, start, end):
        """
        >>> l = RedisList([2.0, "sdfdsf", RedisList([1,2,3])])
        >>> del l[1:2]
        >>> l._load()
        [2.0, <__main__.RedisList object at 0x7f2154a24390>]
        >>> del l[1:12]
        >>> l._load()
        [2.0]
        """
        if self.cache:
            del self.cache[start:end]
        if start == 0:
            self.client.ltrim(self._addr_, end, -1)
            self.client.ltrim(self._type_addr_, end, -1)
        else:
            self.client.eval(DELSLICE_LUA_SCRIPT, 2, self._addr_, self._type_addr_, start, end)

    def __contains__(self, val):
        if self.cache:
            return val in self.cache
        obj, t = get_redis_object_and_type(val)
        idx = self.client.eval(FIND_ITEM_LUA_SCRIPT, 2, self._addr_, self._type_addr_, obj, t)
        if idx < 0:
            return False
        return True

    def append(self, val):
        """
        >>> l = RedisList([1,2.0,True,"abc", RedisStr("xyz")])
        >>> l.append(RedisList([1,2,3]))
        >>> len(l)
        6
        >>> l[5]._load()
        [1, 2, 3]
        """
        if self.cache:
            self.cache.append(val)
        obj, t = get_redis_object_and_type(val)
        with self.client.pipeline() as pipe:
            pipe.rpush(self._addr_, obj)
            pipe.rpush(self._type_addr_, t)
            pipe.execute()

    def extend(self, iterable):
        if isinstance(iterable, RedisList):
            objs, types = iterable._load_objects_and_types()
            # multi value rpush only after redis >= 2.4
            # self.client.rpush(self._addr_, *objs)
            # self.client.rpush(self._type_addr_, *types)
            with self.client.pipeline() as pipe:
                for obj, t in zip(objs, types):
                    pipe.rpush(self._addr_, obj)
                    pipe.rpush(self._type_addr_, t)
                pipe.execute()
        else:            
            if isinstance(iterable, RedisStr):
                iterable = iterable.value()
            for item in iterable:
                obj, t = get_redis_object_and_type(item)
                self.client.rpush(self._addr_, obj)
                self.client.rpush(self._type_addr_, t)

    def pop(self):
        if self.cache:
            self.cache.pop()
        with self.client.pipeline() as pipe:
            pipe.rpop(self._addr_)
            pipe.rpop(self._type_addr_)
            [obj, t] = pipe.execute()
        v = get_value_from_object_and_type(obj, t)
        return v

    def remove(self, val):
        # this is a bit convoluted, don't call this unless necessary
        """
        >>> l = RedisList([2.0, "sdfdsf", RedisList([1,2,3])])
        >>> l.append("abc")
        >>> l.append(12)
        >>> l.remove("abc")
        >>> l._load()
        [2.0, 'sdfdsf', <__main__.RedisList object at 0x7f5110745190>, 12]
        """
        if self.cache:
            self.cache.remove(val)
        obj, t = get_redis_object_and_type(val)
        idx = self.client.eval(FIND_ITEM_LUA_SCRIPT, 2, self._addr_, self._type_addr_, obj, t)

        if idx >= 0:
            with self.client.pipeline() as pipe:
                # First mark the type at index to be deleted, then call LREM
                pipe.lset(self._addr_, idx, DELETE_PLACE_HOLDER)
                pipe.lrem(self._addr_, 1, DELETE_PLACE_HOLDER)
                pipe.lset(self._type_addr_, idx, DELETE_PLACE_HOLDER)
                pipe.lrem(self._type_addr_, 1, DELETE_PLACE_HOLDER)
                pipe.execute()

    def sort(self):
        """
        >>> l = RedisList([1,3, 5, 24, 4, -2])
        >>> l.sort()
        >>> l._load()
        [-2, 1, 3, 4, 5, 24]
        """
        # different from list.sort(), this doesn't accept parameters
        if self.cache:
            self.cache.sort()
        self.client.sort(self._addr_, store=self._addr_) # sort in place

    def index(self, val):
        """
        >>> l = RedisList([2.0, "sdfdsf", RedisList([1,2,3])])
        >>> l.append("2.3")
        >>> 2.3 in l
        False
        >>> "2.3" in l
        True
        >>> l.append(2.3)
        >>> 2.3 in l
        True
        >>> l.index(2.3)
        4L
        >>> l.index("2.3")
        3L
        """
        if self.cache:
            return self.cache.index(val)
        obj, t = get_redis_object_and_type(val)
        idx = self.client.eval(FIND_ITEM_LUA_SCRIPT, 2, self._addr_, self._type_addr_, obj, t)
        return idx

    def reverse(self):
        if self.cache:
            self.cache.reverse()
        self.client.eval(REVERSE_LUA_SCRIPT, 2, self._addr_, self._type_addr_)

    def insert(self,idx, val):
        if self.cache:
            self.cache.insert(idx, val)
        obj, t = get_redis_object_and_type(val)
        self.client.eval(INSERT_LUA_SCRIPT, 2, self._addr_, self._type_addr_, idx, obj, t)

    def count(self, val):
        if self.cache:
            return self.cache.count(val)
        obj, t = get_redis_object_and_type(val)
        return self.client.eval(COUNT_LUA_SCRIPT, 2, self._addr_, self._type_addr_, obj, t)

    # define methods for redis specific commands
    def lpush(self, val):
        if self.cache:
            self.cache.insert(0, val)
        obj, t = get_redis_object_and_type(val)
        self.client.lpush(self._addr_, obj)
        self.client.lpush(self._type_addr_, t)

    def lpop(self):
        if self.cache:
            v = self.cache[0]
            del self.cache[0]
            return v
        obj = self.client.lpop(self._addr_)
        t = self.client.lpop(self._type_addr_)
        return get_value_from_object_and_type(obj, t)
