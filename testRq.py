#!/usr/bin/python
import redis

r = redis.Redis(host='localhost', port=6379, db=0)
a = '1452510030'

print r.lrange(a,0,-1)
