from dbase import *
import random, string
import contextlib
from utils import *

POP_ITEM_LUA_SCRIPT = """
local addr = KEYS[1]
local taddr = KEYS[2]
local k = ARGV[1]
v = redis.call('hget', addr, k)
redis.call('hdel', addr, k)
redis.call('hdel', taddr, k)
return v
"""

class RedisDict(dbase):
    def __init__(self, _dict=None):
        dbase.__init__(self)        
        if _dict:
            self.update(_dict)

    def initialize(self):
        self._type_addr_ = "_type_" + self._addr_
        self._type_ = "dmem:dict"
        self.cache = None

    def _load_objects_and_types(self):
        objdict = self.client.hgetall(self._addr_)
        tdict = self.client.hgetall(self._type_addr_)
        return objdict, tdict

    def _load(self):
        od, td = self._load_objects_and_types()
        _dict = {}
        for key in od:
            obj = od[key]
            t = td[key]
            v = get_value_from_object_and_type(obj, t)
            _dict[key] = v
        return _dict

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
        return iter(self.keys())

    def __contains__(self, key):
        if self.cache:
            return key in self.cache
        return self.client.hexists(self._addr_, key)

    def __len__(self):
        if self.cache:
            return len(self.cache)
        return self.client.hlen(self._addr_)

    def __getitem__(self, key):
        if self.cache:
            return self.cache[key]
        with self.client.pipeline() as pipe:
            pipe.hget(self._addr_, key)
            pipe.hget(self._type_addr_, key)
            [obj, t] = pipe.execute()
        v = get_value_from_object_and_type(obj, t)
        return v

    def __setitem__(self, key, value):
        if not isinstance(key, basestring):
            raise KeyError("Only string key is supported")
        if self.cache:
            self.cache[key] = value
        obj, t = get_redis_object_and_type(value)
        with self.client.pipeline() as pipe:
            pipe.hset(self._addr_, key, obj)
            pipe.hset(self._type_addr_, key, t)
            pipe.execute()

    def __delitem__(self, key):
        if self.cache:
            del self.cache[key]
        with self.client.pipeline() as pipe:
            pipe.hdel(self._addr_, key)
            pipe.hdel(self._type_addr_, key)
            pipe.execute()

    def clear(self):
        if self.cache:
            self.cache = {}
        self.client.delete(self._addr_)
        self.client.delete(self._type_addr_)

    def copy(self):
        # shallow copy of self
        return RedisDict(self._load())

    @classmethod
    def fromkeys(cls, keys, val):
        d = {key:val for key in keys}
        return RedisDict(d)

    def get(self, key, default=None):
        v = self.__getitem__(key)
        if not v:
            return default
        return v

    def has_key(self, key):
        return key in self

    def items(self):
        if self.cache:
            return self.cache.items()
        d = self._load()
        return d.items()

    def keys(self):
        if self.cache:
            return self.cache.keys()
        return self.client.hkeys(self._addr_)

    def values(self):
        if self.cache:
            return self.cache.values()
        with self.client.pipeline() as pipe:
            pipe.hvals(self._addr_)
            pipe.hvals(self._type_addr_)
            objs, ts = pipe.execute()
        return [get_value_from_object_and_type(obj, t) for obj, t in zip(objs, ts)]
    
    # Doesn't seem necessary to implement iterator
    def iteritems(self):
        return self.items()
    def iterkeys(self):
        return self.keys()
    def itervalues(self):
        return self.values()

    def update(self, updates): 
        if self.cache:
            self.cache.update(updates)
        if hasattr(updates, "items"): # has to be a list [(k, v), (k, v) ...]
            updates = updates.items()
        objdict ={} 
        tdict = {}
        for k, v in updates:
            obj, t = get_redis_object_and_type(v)
            objdict[k] = obj
            tdict[k] = t        
        with self.client.pipeline() as pipe:
            pipe.hmset(self._addr_, objdict)
            pipe.hmset(self._type_addr_, tdict)
            pipe.execute()

    def setdefault(self, k, d):
        if self.cache:
            self.cache.setdefault(k, d)
        if self.has_key(k):
            return self.__getitem__(k)
        else:
            self.__setitem__(k, d)
            return d

    def pop(self, k, d=None):
        if self.cache:
            self.cache.pop(k)
        v = self.client.eval(POP_ITEM_LUA_SCRIPT, 2, self._addr_, self._type_addr_, k)
        return v
