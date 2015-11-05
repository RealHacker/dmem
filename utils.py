import redis
import json

class RedisClientPool(object):
    singleton = None
    def __init__(self):
        self.clients = {}
        self.names = []
    
    @classmethod
    def get_pool(cls):
        if not cls.singleton:
            cls.singleton = RedisClientPool()
        return cls.singleton

    def load_config(self, config):
        """ config has to be either a dict or a file name holding the config dict:
        {
            "redis1": {"host":"192.168.1.1", "port":3279, "db":0},
            "redis2": {"host":"192.168.1.2", "port":3279, "db":0}
        }
        """
        if isinstance(config, basestring):
            try:
                configstr = open(config, "r").read()
                config = json.loads(configstr)
            except:
                raise ConfigReadError()
        # now config has to be a dict
        if not isinstance(config, dict):
            raise Exception("Invalid config passed to load_config")
        for k in config:
            v = config[k]
            self.clients[k] = RedisClient(v["host"], v["port"], v["db"])
            self.clients[k].ping()
            self.names.append(k)

    def get_client(self, name):
        if name not in self.clients:
            raise InvalidRedisClientName()
        return self.clients[name]

class RedisClient(object): # A wrapper of redis client for debugging, etc
    def __init__(self, host, port, db):
        self.client = redis.StrictRedis(host=host, port=port, db=db)

    def __getattr__(self, attr):
        print "Calling method %s of redis client"%attr
        return getattr(self.client, attr)
        
# Configure the client pool singleton instance
RedisClientPool.get_pool().load_config({"redis1": {"host":"192.168.1.7", "port": 6379, "db":0}})

class ConfigReadError(Exception):
    """Raised when failed to read or parse config file"""

class RedisNotInitialized(Exception):
    """Raised when redis client is not ready"""

class RedisOperationFailure(Exception):
    """Raised when redis command returns failure"""
    
class RedisNestedTypeError(Exception):
    """Raised when a nested type within dbase is neither atomic type or a dbase type"""
    
class InvalidRedisClientName(Exception):
    """Raised when the redis client of that name is not found"""