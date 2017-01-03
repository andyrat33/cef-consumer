import redis
import threading
import datetime

config = {
    'host': 'docker2',
    'port': 6379,
    'db': 0,
}

r = redis.StrictRedis(**config)
counter = r.get(name="count")
print("Current count : {}".format(int(counter)))
r.hmset("test", dict(age=31, size=21))
def storeStats():
    threading.Timer(5.0, storeStats).start()
    

#storeStats()
import threading

def do_every (interval, worker_func, iterations = 0):
    if iterations != 1:
        threading.Timer (
            interval,
            do_every, [interval, worker_func, 0 if iterations == 0 else iterations-1]
            ).start ()

    worker_func ()

def print_hw ():
    print("hello world")

def print_so ():
    print("stackoverflow")


# call print_so every second, 5 times total
do_every(1, print_so, 5)

# call print_hw two times per second, forever
do_every(25, print_hw)
