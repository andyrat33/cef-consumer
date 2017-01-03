import redis

config = {
    'host': 'docker2',
    'port': 6379,
    'db': 0,
}

r = redis.StrictRedis(**config)
counter = r.get(name="count")
print("Current count : {}".format(int(counter)))
r.hmset("test", dict(age=31, size=21))

print()
