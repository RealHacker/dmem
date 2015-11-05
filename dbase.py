from utils import RedisClientPool
import random, string

PRESENCE_PREFIX = "_exists_"

class dbase(object):
    def __init__(self):
        # first choose a node randomly
        poll = RedisClientPool.get_pool()
        node_names = poll.names
        self._node_ = random.choice(node_names)
        self.client = poll.get_client(self._node_)
        # then pick a random address for the object
        self._addr_ = self.get_a_valid_redis_addr()

    def addr(self):
        return self._addr_

    @classmethod
    def _from_addr(cls, addr):
        obj = cls.__new__(cls)
        obj._addr_ = addr
        obj._node_ = cls.get_node_from_addr(addr)
        obj.client = RedisClientPool.get_pool().get_client(obj._node_)
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
            presence_key = PRESENCE_PREFIX + key
            present = self.client.getset(presence_key, 1)
            if not present:
                return self._node_ + ":" + key

