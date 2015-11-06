from dbase import dbase
import random, string
import contextlib
from utils import *

class RedisSet(dbase):
    def __init__(self, _elements=None):
        dbase.__init__(self)
        self._type_ = "dmem:set"
        self.cache = None
        if _elements:
            self.update(_elements)

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
    def _get_object_from_value_type(v, t):
        return t+"#"+v

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
        v = self._get_object_from_value_type(obj, t)
        return self.client.sismember(self._addr_, v)

    def __len__(self):
        if self.cache:
            return len(self.cache)
        return self.client.scard(self._addr_)

    def __and__(self, other):
        if isinstance(other, RedisSet):
            objs = self.client.sinter(self._addr_, other._addr_)
            intersection = set()
            for obj in objs:
                v, t = self._get_value_type_from_object(obj)
                v = get_value_from_object_and_type(v, t)
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
        else:
            newitems = []
            for item in other:
                obj, t = get_redis_object_and_type(item)
                newitems.append(self._get_object_from_value_type(obj, t))
            self.client.sadd(self._addr_, *newitems)


