# Dmem - Distributed Memory with Redis #

**Redis**, known as a data structure server, is often used to store and retrieve distributed data, mostly for caching or distributing messages. Yet solutions using redis have some limitations:

1. Client code has to use explicit commands (HGET, SMEMBERS, etc) to store and retrieve objects, instead of native data structure operators and methods that people are familiar with. 
2. Redis can store only one level of data structures - a list, a dict, or a set of strings. Nested structures like a list of dicts, a dict with list values, are not natively supported.

## What is Dmem? ##

**Dmem** is a thin wrapper over Redis, that provides a transparent API in python to store/retrieve data on remote servers:

1. It uses redis API behind the scenes, but provides data operations via well-known builtin type operators and methods.
2. Nested structures of multiple levels are supported.
3. A simple reference counting mechanism is in place, so when a remote object is no used by any client code, it is deleted from Redis.
4. A distributed lock is in place, to protect a dmem object from multiple access from different clients

## Talk is cheap, show me the code ##

Dmem provides 5 new types that maps python container types to Redis data structures: `RedisStr`, `RedisList`, `RedisDict`, `RedisSet`, `RedisObject`, you can use them exactly like the native counterparts(`str/list/dict/set/object`), except of course, the data is stored in another Redis node.

Before instantiating any dmem objects, set up your Redis nodes configuration:

    from dmem import *
    # Configure your redis instances pool
    RedisClientPool.get_pool().load_config({
        "redis1": {"host":"192.168.1.1", "port": 6379, "db":0},
		"redis2": {"host":"192.168.1.2", "port": 6379, "db":0},
		"redis3": {"host":"192.168.1.3", "port": 6379, "db":0},
    })

Now it is time to play:
    
    mylist = RedisList([1, "abc", 3.1415]) 
    # Now the list is alive on Redis
    print mylist[1]  	# actually retrieved with LGET command
    # output: 'abc'
    print mylist[0:2]	# LRANGE command
    # output: [1, 'abc']
    del mylist[0]		# LREM command
    print mylist[:]
    # output: ["abc", 3.1415]
    s = RedisStr("Redis string")
    # Now the string lives in Redis
    mylist.append(s)    # APPEND command
    print len(mylist)   # STRLEN command
    # output: 3
    print mylist[-1].getvalue()  # GET command
    # output: 'Redis string'
    mydict = RedisDict({"a":1234}) # HSET commands
    # Now the dict lives in Redis as a hashmap
    mydict["list"] = mylist 	# HGET command
    print mydict.keys()         # HKEYS command
    # output: ['a', 'list']
    for k,v in mydict.items():  # HGETALL command
        print k, v
    # output: a 1234
    # output: list <redislist.RedisList object at 0x01CDEF30>
    print mydict["list"][0]
    # output: 'abc'
    obj = RedisObject()
    # For redis object, the attributes are stored in Redis
    obj.attr1 = "abc"
    obj.attr2 = mydict
    print obj.attr2['list'][0]
	# output: 'abc'
    
As you can see, you can use dmem objects almost anywhere a native python type is used. And for applications that need a lot of main memory, you don't need to worry about OOM problems, as long as you have enough Redis nodes configured. 

The only downside to using dmem objects is: data access is less efficient than main memory, as a network roundtrip is involved. Sometimes, multiple roundtrips can be saved, if you preload a dmem object in one go. For instance, when iterating over a RedisList:

    # without preloading
    for v in mylist:
       print v 		# Every item access is a LGET command
    # with preloading
    with mylist.loaded() as cache:
	   for v in cache:
	     print v	

## Under the hood ##
Dmem is fairly straightforward:

- It gives each dmem object a unique address, and use it as KEY in redis. 
- It maps data structure operators/methods to Redis commands, pipeline/lua scripts are used to pack multiple command into one network request.
- Both value and data type are preserved, if you save `1.23` in a dmem container, you will get a `float` back, instead of a `string` `'1.23'`

If you want to see what exactly is happening, just turn on debug, and see all Redis commands printed:

	>>> dmem.enable_debug()
	>>> l = dmem.RedisList([1,2,3])
	Calling command getset of redis client
	Calling command rpush of redis client
	Calling command rpush of redis client
	Calling command rpush of redis client
      

   




