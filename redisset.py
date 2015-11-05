from dbase import dbase
import random, string
import contextlib
from utils import *

class RedisSet(dbase):
    def __init__(self, _elements=None):
        dbase.__init__(self)
        self._type_addr_ = "_type_" + self._addr_
        self._type_ = "dmem:set"
        self.cache = None
        if _elements:
            self.update(_elements)

    def _load_objects_and_types(self):
        objs = self.client.smembers(self._addr_)
        ts = self.client.hgetall(self._type_addr_)
        return objs, ts

    def _load(self):
        objs, ts = self._load_objects_and_types()
        values = set()
        for obj in objs:
            t = ts[obj]
            v = get_value_from_object_and_type(obj, t)
            values.add(v)
        return values
    
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
        obj, t = get_redis_object_and_type(element)
        with self.client.pipeline() as pipe:
            pipe.sismember(self._addr_, obj)
            pipe.hget(self._type_addr_, obj)
            isMember, tt = pipe.execute()
        return isMember and tt==t

    def __len__(self):
        if self.cache:
            return len(self.cache)
        return self.client.scard(self._addr_)

    def __and__(self, other):
        if isinstance(other, RedisSet):
            objs = self.client.sinter(self._addr_, other._addr_)
            typehash = self.client.hgetall(self._type_addr_)
            intersection = set()
            for obj in objs:
                v = get_value_from_object_and_type(obj, typehash[obj])
                intersection.add(v)
        else:
            if self.cache:
                intersection = self.cache&other
            else:
                intersection = self._load()&other
        return RedisSet(intersection)

    def update(self, other):
        if isinstance(other, RedisSet):
            self.client.sunionstore(self._addr_, self._addr_, other._addr_)
