import redis

class RedisClient(object):
    def __init__(self, client):
        self.client = client

    def __getattr__(self, attr):
        print "Calling method %s of redis client"%attr
        return getattr(self.client, attr)
        
def initialize_redis(host="127.0.0.1", port=6379, db=0):
    _client = redis.StrictRedis(host=host, port=port, db=db)
    redis_client = RedisClient(_client)
    return redis_client

class RedisNotInitialized(Exception):
    """Raised when redis client is not ready"""

class RedisOperationFailure(Exception):
    """Raised when redis command returns failure"""
    
class RedisNestedTypeError(Exception):
    """Raised when a nested type within dbase is neither atomic type or a dbase type"""
    