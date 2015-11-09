from dbase import *
import random, string
import contextlib
from utils import *

TEMP_PREFIX = "_temp_"

XOR_LUA_SCRIPT = """
local key1 = KEYS[1]
local key2 = KEYS[2]
local diff1 = redis.call('sdiff', key1, key2)
local diff2 = redis.call('sdiff', key2, key1)
local len1 = #diff1
for i=1,#diff2 do
    diff1[len1+i] = diff2[i]
end
return diff1
"""

XORSTORE_LUA_SCRIPT = """
local key1 = KEYS[1]
local key2 = KEYS[2]
local intersection = redis.call('sinter', key1, key2)
redis.call('sunionstore', key1, key1, key2)
for i=1,#intersection do
    redis.call('srem', key1, intersection[i])
end
"""

class RedisSet(dbase):
    def __init__(self, _elements=None):
        dbase.__init__(self)
        if _elements:
            self.update(_elements)

    def initialize(self): # initializations specific to RedisSet
        self._type_ = "dmem:set"
        self.cache = None

    def _load_objects_and_types(self):
        objs = self.client.smembers(self._addr_)
        tuples = []
        for obj in objs:
            tuples.append(self._get_value_type_from_object(obj))
        return tuples

    def _load(self):
        tuples = self._load_objects_and_types()
        values = set([get_value_from_object_and_type(o, t) for o,t in tuples])
        return values
    
    @staticmethod
    def _get_value_type_from_object(obj):
        split_at = obj.index("#")
        return (obj[split_at+1:], obj[:split_at])

    @staticmethod
    def get_value_from_redis(r):
        v, t = RedisSet._get_value_type_from_object(r)
        return get_value_from_object_and_type(v, t)

    @staticmethod
    def _get_object_from_value_type(v, t):
        return t+"#"+v

    @staticmethod
    def convert_value_into_redis(v):
        obj, t = get_redis_object_and_type(v)
        return RedisSet._get_object_from_value_type(obj, t)

    @contextlib.contextmanager
    def loaded(self):
        self.cache = self._load()
        try:
            yield self.cache
        finally:
            self.cache = None

    def __contains__(self, element):
        if self.cache:
            return element in self.cache
        v = self.convert_value_into_redis(element)
        return self.client.sismember(self._addr_, v)

    def __len__(self):
        if self.cache:
            return len(self.cache)
        return self.client.scard(self._addr_)

    def __iter__(self):
        return iter(self._load())

    def __and__(self, other):
        if isinstance(other, RedisSet):
            objs = self.client.sinter(self._addr_, other._addr_)
            intersection = set()
            for obj in objs:                
                v = self.get_value_from_redis(obj)
                intersection.add(v)
        else:
            if self.cache:
                intersection = self.cache&other
            else:
                intersection = self._load()&other
        return RedisSet(intersection)

    def __iand__(self, other):
        if isinstance(other, RedisSet):
            self.client.sinterstore(self._addr_, self._addr_, other._addr_)
            if self.cache:
                self.cache = self._load() # reload local cache
        else:
            elements = [self.convert_value_into_redis(v) for v in other]
            with self.client.pipeline() as pipe:
                tempkey = TEMP_PREFIX + self._addr_
                pipe.sadd(tempkey, *elements)
                pipe.sinterstore(self._addr_, self._addr_, tempkey)
                pipe.delete(tempkey)
                pipe.execute()
            if self.cache:
                self.cache &= other
        return self

    def __or__(self, other):
        if isinstance(other, RedisSet):
            objs = self.client.sunion(self._addr_, other._addr_)
            union = set()
            for obj in objs:
                v = self.get_value_from_redis(obj)
                union.add(v)
        else:
            if self.cache:
                union = self.cache|other
            else:
                union = self._load()|other
        return RedisSet(union)

    def __ior__(self, other):
        if isinstance(other, RedisSet):
            self.client.sunionstore(self._addr_, self._addr_, other._addr_)
            if self.cache:
                self.cache = self._load()
        else:
            elements = [self.convert_value_into_redis(v) for v in other]
            self.client.sadd(self._addr_, *elements)
            if self.cache:
                self.cache |= other
        return self

    def __sub__(self, other):
        if isinstance(other, RedisSet):
            objs = self.client.sdiff(self._addr_, other._addr_)
            diff = set()
            for obj in objs:
                v = self.get_value_from_redis(obj)
                diff.add(v)
        else:
            if self.cache:
                diff = self.cache-other
            else:
                diff = self._load()-other
        return RedisSet(diff)

    def __isub__(self, other):
        if isinstance(other, RedisSet):
            self.client.sdiffstore(self._addr_, self._addr_, other._addr_)
            if self.cache:
                self.cache = self._load()
        else:
            elements = [self.convert_value_into_redis(v) for v in other]
            self.client.srem(self._addr_, *elements)
            if self.cache:
                self.cache -= other
        return self

    def __xor__(self, other):
        if isinstance(other, RedisSet):
            objs = self.client.eval(XOR_LUA_SCRIPT, 2, self._addr_, other._addr_)
            xor = set()
            for obj in objs:
                v = self.get_value_from_redis(obj)
                xor.add(v)
        else:
            if self.cache:
                xor = self.cache^other
            else:
                xor = self._load()^other
        return RedisSet(xor)

    def __ixor__(self, other):
        if isinstance(other, RedisSet):
            self.client.eval(XORSTORE_LUA_SCRIPT, 2, self._addr_, other._addr_)
            if self.cache:
                self.cache = self._load()
        else:
            elements = [self.convert_value_into_redis(v) for v in other]
            with self.client.pipeline() as pipe:
                tempkey = TEMP_PREFIX + self._addr_
                pipe.sadd(tempkey, *elements)
                pipe.eval(XORSTORE_LUA_SCRIPT, 2, self._addr_, tempkey)
                pipe.delete(tempkey)
                pipe.execute()
            if self.cache:
                self.cache ^= other
        return self

    def update(self, other):
        self.__ior__(other)

    def add(self, ele):
        if self.cache:
            self.cache.add(ele)
        r = self.convert_value_into_redis(ele)
        self.client.sadd(self._addr_, r)

    def clear(self):
        if self.cache:
            self.cache.clear()
        self.client.delete(self._addr_)

    def copy(self):
        return RedisSet(self._load())

    def difference(self, other):
        return self.__sub__(other)

    def difference_update(self, other):
        return self.__isub__(other)

    def intersection(self, other):
        return self.__and__(other)

    def intersection_update(self, other):
        return self.__iand__(other)

    def union(self, other):
        return self.__or__(other)

    def symmetric_difference(self, other):
        return self.__xor__(other)

    def symmetric_difference_update(self, other):
        return self.__ixor__(other)

    def discard(self, ele):
        if self.cache:
            self.cache.discard(ele)
        r = self.convert_value_into_redis(ele)
        self.client.srem(self._addr_, r)

    def isdisjoint(self, other):
        return len(self.intersection(other))==0

    def issubset(self, other):
        return len(self.difference(other))==0

    def issuperset(self, other):
        return other.issubset(self)

    def remove(self, ele): # ele must be a member
        if self.cache:
            if ele not in self.cache:
                raise KeyError("element not in set")
            self.cache.remove(ele)
        r = self.convert_value_into_redis(ele)
        if not self.client.sismember(self._addr_, r):
            raise KeyError("element not in set")
        self.client.srem(self._addr_, r)

    def pop(self):
        r = self.client.spop(self._addr_)
        ele = self.get_value_from_redis(r)
        if self.cache:
            self.cache.discard(ele)
        return ele
