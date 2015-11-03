import utils
from dbase import dbase
import random, string
import contextlib
from utils import initialize_redis, RedisNotInitialized, RedisOperationFailure

redis_client = initialize_redis()
used_redis_addresses = set()

def get_a_valid_redis_addr(t):
    while True:
        key = t+"_"+ ''.join([random.choice(string.digits+string.lowercase
                                           ) for i in range(10)])
        if key not in used_redis_addresses:
            used_redis_addresses.add(key)
            return key

def get_redis_type_from_addr(addr):
    return "dmem:" + addr.split("_")[0]

def get_redis_object_and_type(v):    
    if isinstance(v, bool):
        return "1" if v else "0", "bool"
    atom_types = [(int, "int"), (long, "long"), (float, "float"), (basestring, "str")]
    for atype, type_name in atom_types:
        if isinstance(v, atype):
            return str(v), type_name
    if isinstance(v, dbase):        
        return v.addr(), get_redis_type_from_addr(v.addr())
    else:
        raise RedisNestedTypeError("Nested type should either be an atomic type (int, float, str, etc) or a Redis Type")

def get_value_from_object_and_type(obj, t):
    if t == "str":
        return obj
    elif t == "int":
        return int(obj)
    elif t == "long":
        return long(obj)
    elif t == "float":
        return float(obj)
    elif t == "bool":
        return bool(int(obj))
    elif t == "dmem:str":
        return RedisStr._from_addr(obj)
    elif t == "dmem:list":
        return RedisList._from_addr(obj)
    else:
        return None

INDEXOF_LUA_SCRIPT = """
local key = KEYS[1]
local obj = ARGV[1]
local items = redis.call('lrange', key, 0, -1)
for i = 1, #items do
    if items[i] == obj then
        return i - 1
    end
end 
return -1
"""

DELETE_PLACE_HOLDER = "__TO_BE_DELETED__"

class RedisStr(dbase):
    def __init__(self, s=None):
        self._addr_ = get_a_valid_redis_addr("str")
        # save when initializing
        if not redis_client:
            raise RedisNotInitialized()
        if s:
            ret = redis_client.set(self._addr_, s)
            if not ret:
                raise RedisOperationFailure()
        
    def getvalue(self):
        # refresh the value
        s = redis_client.get(self._addr_)
        return s

    def setvalue(self, s):
        ret = redis_client.set(self._addr_, s)
        if not ret:
            raise RedisOperationFailure()

    def __iadd__(self, more):
        if isinstance(more, RedisStr):
            more = more.getvalue()
        if not isinstance(more, basestring):
            raise TypeError("The argument is not a string")
        ret = redis_client.append(self._addr_, more)
        if not ret:
            raise RedisOperationFailure()
        return self

    def __add__(self, more):
        # return a new RedisStr
        s = self.getvalue()
        if isinstance(more, RedisStr):
            more = more.getvalue()
        news = RedisStr(s+more)
        return news

    def __str__(self):
        return s.getvalue()

    def __contains__(self, sub):
        return sub in self.getvalue()

    def __getslice__(self, i, j):
        ret = redis_client.getrange(self._addr_, i, j)
        if not ret:
            raise RedisOperationFailure()
        return ret

    def __setslice__(self, i, j, val):
        ret = redis_client.setrange(self._addr_, i, j, val)
        if not ret:
            raise RedisOperationFailure()
    
    def __len__(self):
        ret = redis_client.strlen(self._addr_)
        return ret

    def __getitem__(self, idx):
        s = self.getvalue()
        return s[idx]

    def __getattr__(self, attr):
        # delegate everything else to string value
        s = self.getvalue()
        if hasattr(s, attr):
            return getattr(s, attr)
        
class RedisList(dbase):
    def setaddr(self, addr):
        self._addr_ = addr
        self._type_addr_ = "_type_" + self._addr_

    def __init__(self, _list=None):
        """
        >>> l = RedisList([1,2.0,True,"abc"])
        >>> l._load()
        [1, 2.0, True, 'abc']
        >>> l._load_objects_and_types()
        (['1', '2.0', '1', 'abc'], ['int', 'float', 'bool', 'str'])
        """
        self.setaddr(get_a_valid_redis_addr("list"))        
        self.cache = None
        # save when initializing
        if not redis_client:
            raise RedisNotInitialized()
        if _list:
            self.extend(_list)
        
    def _load_objects_and_types(self):
        objects = redis_client.lrange(self._addr_, 0, -1)
        types = redis_client.lrange(self._type_addr_, 0, -1)
        return objects, types

    def _load(self):
        objs, types = self._load_objects_and_types()
        assert(len(objs) == len(types))
        values = []
        for i in range(len(objs)):
            value = get_value_from_object_and_type(objs[i], types[i])
            values.append(value)
        return values

    @contextlib.contextmanager
    def loaded(self):
        self.cache = self._load()
        try: 
            yield self.cache
        finally:
            self.cache = None
    
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
            return redis_client.llen(self._addr_)

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
        with redis_client.pipeline() as pipe:
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
        with redis_client.pipeline() as pipe:
            pipe.lset(self._addr_, idx, obj)
            pipe.lset(self._type_addr_, idx, t)
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
        objs = redis_client.lrange(self._addr_, start, end-1)
        ts = redis_client.lrange(self._type_addr_, start, end-1)
        values = []
        for obj, t in zip(objs, ts):
            values.append(get_value_from_object_and_type(obj, t))
        return values

    def __contains__(self, val):
        if self.cache:
            return val in self.cache
        obj, t = get_redis_object_and_type(val)
        idx = redis_client.eval(INDEXOF_LUA_SCRIPT, 1, self._addr_, obj)
        if idx < 0:
            return False
        tt = redis_client.lindex(self._type_addr_, idx)
        # todo: this is incorrect
        return t == tt

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
        with redis_client.pipeline() as pipe:
            pipe.lset(self._addr_, idx, DELETE_PLACE_HOLDER)
            pipe.lrem(self._addr_, 1, DELETE_PLACE_HOLDER)
            pipe.lset(self._type_addr_, idx, DELETE_PLACE_HOLDER)
            pipe.lrem(self._type_addr_, 1, DELETE_PLACE_HOLDER)
            pipe.execute()

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
        with redis_client.pipeline() as pipe:
            pipe.rpush(self._addr_, obj)
            pipe.rpush(self._type_addr_, t)
            pipe.execute()

    def extend(self, iterable):
        if isinstance(iterable, RedisList):
            objs, types = iterable._load_objects_and_types()
            # multi value rpush only after redis >= 2.4
            # redis_client.rpush(self._addr_, *objs)
            # redis_client.rpush(self._type_addr_, *types)
            with redis_client.pipeline() as pipe:
                for obj, t in zip(objs, types):
                    redis_client.rpush(self._addr_, obj)
                    redis_client.rpush(self._type_addr_, t)
                pipe.execute()
        else:            
            if isinstance(iterable, RedisStr):
                iterable = iterable.value()
            for item in iterable:
                obj, t = get_redis_object_and_type(item)
                redis_client.rpush(self._addr_, obj)
                redis_client.rpush(self._type_addr_, t)

    def pop(self):
        if self.cache:
            self.cache.pop()
        with redis_client.pipeline() as pipe:
            pipe.rpop(self._addr_)
            pipe.rpop(self._type_addr_)
            [obj, t] = pipe.execute()
        v = get_value_from_object_and_type(obj, t)
        return v

    def remove(self, val):
        # this is a bit convoluted, don't call this unless necessary
        if self.cache:
            self.cache.remove(val)
        obj, t = get_redis_object_and_type(val)
        with redis_client.pipeline() as pipe:
            pipe.eval(INDEXOF_LUA_SCRIPT, 1, self._addr_, obj)
            pipe.lrem(self._addr_, obj, 1)
            [idx, removed] = pipe.execute()
        if idx >= 0 and removed:
            with redis_client.pipeline() as pipe:
                # First mark the type at index to be deleted, then call LREM
                pipe.lset(self._type_addr_, idx, DELETE_PLACE_HOLDER)
                pipe.lrem(self._type_addr_, DELETE_PLACE_HOLDER, 1)

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
        redis_client.sort(self._addr_, store=self._addr_) # sort in place

    def index(self, val):
        if self.cache:
            return self.cache.index(val)
        obj, t = get_redis_object_and_type(val)
        idx = redis_client.eval(INDEXOF_LUA_SCRIPT, 1, self._addr_, obj)
        # This is incorrect
        return idx

    def reverse(self):
        if self.cache:
            self.cache.reverse()
        # TODO: use lua script

    def insert(self,idx, val):
        pass

