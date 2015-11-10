from utils import RedisClientPool
import random, string

REF_PREFIX  = "_ref_"
LOCK_PREFIX = "_lock_"

UNLOCK_LUA_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
"""
class dbase(object):
    def __init__(self):
        # first choose a node randomly
        poll = RedisClientPool.get_pool()
        node_names = poll.names
        self._node_ = random.choice(node_names)
        self.client = poll.get_client(self._node_)
        # then pick a random address for the object
        self._addr_ = self.get_a_valid_redis_addr()
        self.initialize()

    def initialize(self): # initialization specific to each subtype
        pass

    def addr(self):
        return self._addr_

    @classmethod
    def _from_addr(cls, addr):
        obj = cls.__new__(cls)
        obj._addr_ = addr
        obj._node_ = cls.get_node_from_addr(addr)
        obj.client = RedisClientPool.get_pool().get_client(obj._node_)
        obj.initialize()
        # when instantiate a redis object from address, increment the counter
        obj._incr_refcnt() 
        return obj

    @classmethod
    def get_node_from_addr(cls, addr):
        return addr.split(":")[0]

    def __eq__(self, other):
        if isinstance(other, dbase):
            return self._addr_ == other._addr_
        return False

    def __ne__(self, other):
        if isinstance(other, dbase):
            return self._addr_ != other._addr_
        return True

    def get_a_valid_redis_addr(self):
        while True:
            key = ''.join([random.choice(string.digits+string.lowercase) for i in range(10)])
            presence_key = REF_PREFIX + self._node_  +":" + key
            present = self.client.getset(presence_key, 1)
            if not present:
                return self._node_ + ":" + key

    def _incr_refcnt(self):
        presence_key = REF_PREFIX + self._addr_
        refcnt = self.client.incr(presence_key)

    def __del__(self):
        # This implements a reference couting on redis, each reference represents a node using the object
        # when the counter reaches 0, the redis key is removed
        # also the presence key is open for address allocation
        presence_key = REF_PREFIX + self._addr_
        refcnt = self.client.decr(presence_key)
        if not refcnt:
            self.destroy()

    def destroy(self):
        # should be overrided if subclass needs to destroy other keys
        self.client.delete(self._addr_)

    def lock(self, ttl=6):
        # add a distributed lock on redis for this object
        lock_key = LOCK_PREFIX + self._addr_
        self._lockval = ''.join([random.choice(string.digits+string.lowercase) for i in range(6)])
        while True:
            result = self.client.set(lock_key, self._lockval, nx=True, px=ttl)
            if result:
                return 

    def unlock(self):
        lock_key = LOCK_PREFIX + self._addr_
        self.client.eval(UNLOCK_LUA_SCRIPT, 1, lock_key, self._lockval)

def get_redis_object_and_type(v):    
    if isinstance(v, bool):
        return "1" if v else "0", "bool"
    atom_types = [(int, "int"), (long, "long"), (float, "float"), (basestring, "str")]
    for atype, type_name in atom_types:
        if isinstance(v, atype):
            return str(v), type_name
    if isinstance(v, dbase):        
        return v.addr(), v._type_
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
        from redisstr import RedisStr
        return RedisStr._from_addr(obj)
    elif t == "dmem:list":
        from redislist import RedisList
        return RedisList._from_addr(obj)
    elif t == "dmem:dict":
        from redisdict import RedisDict
        return RedisDict._from_addr(obj)
    elif t == "dmem:set":
        from redisset import RedisSet
        return RedisSet._from_addr(obj)
    elif t == "dmem:object":
        from redisobj import RedisObject
        return RedisObject._from_addr(obj)
    else:
        return None

def test():
    # Configure the client pool singleton instance
    RedisClientPool.get_pool().load_config({"redis1": {"host":"127.0.0.1", "port": 6379, "db":0}})


