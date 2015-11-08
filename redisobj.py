from dbase import *
import contextlib
from utils import *

class RedisObject(dbase):
	def __init__(self):
		dbase.__init__(self)
		self.__dict__['_type_'] = "dmem:object"
		self.__dict__['_type_addr_'] = "_type_" + self._addr_
		self.__dict__['cache'] = None
		self.__dict__['refs'] = set()

	def _load_objects_and_types(self):
		objdict = self.client.hgetall(self._addr_)
		tdict = self.client.hgetall(self._type_addr_)
		return objdict, tdict

	def _load(self):
	    od, td = self._load_objects_and_types()
	    assert(len(od) == len(td))
	    _attributes = {}
	    for key in od:
	        obj = od[key]
	        t = td[key]
	        v = get_value_from_object_and_type(obj, t)
	        _attributes[key] = v
	    return _attributes

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

	def __setattr__(self, name, val):
		if callable(val) or name in ['_node_', 'client', '_addr_', '_type_', 'cache']:
			self.__dict__[name] = val
		else:
			v, t = get_redis_object_and_type(val)
			if t.startswith("dmem"):
				self.__dict__['refs'].add(val)
			with self.client.pipeline() as pipe:
				pipe.hset(self._addr_, name, v)
				pipe.hset(self._type_addr_, name, t)
				pipe.execute()
			if self.cache:
				self.cache[name] = val

	def __getattr__(self, name):
		if name in self.__dict__:
			return self.__dict__[name]
		if "cache" in self.__dict__:
			if name not in self.__dict__["cache"]:
				raise AttributeError("attribute not found")
			return self.__dict__["cache"][name]
		else:
			with self.client.pipeline() as pipe:
				pipe.hget(self._addr_, name)
				pipe.hget(self._type_addr_, name)
				[v, t] = pipe.execute()
		attr = get_value_from_object_and_type(v, t)
		return attr

	def __delattr__(self, name):
		if name in self.__dict__:
			del self.__dict__[name]
			return
		if self.cache:
			del self.cache[name]
		with self.client.pipeline() as pipe:
			pipe.hdel(self._addr_, name)
			pipe.hdel(self._type_addr_, name)
			pipe.execute()
