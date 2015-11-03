# Dmem - Distributed Memory #

We can use memcached or redis to store and retrieve data on different servers. But solution with redis have some limitations:

1. Client has to use explicit commands (like LGET) to store and retrieve objects.
2. Redis can store only single-level data structure, like a list, a dict, or a set. Nested structures like a list of dicts is not natively supported.

Dmem is a transparent API to store/retrieve data on another server:

1. It uses redis API under the hood, but provides data operations via well-known builtin type operators and functions.
2. Nested structures of multiple levels are supported  

