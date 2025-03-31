import threading
from time import sleep
import random

N = 5
BUFSIZE = 10
buf = [-1] * BUFSIZE
nextin, nextout = 0, 0


mutex = threading.Lock()
empty = threading.Semaphore(BUFSIZE)
full = threading.Semaphore(0)


def producer(n):
    global buf, nextin

    for _ in range(2 * N):
        empty.acquire()
        mutex.acquire()
        item = _
        buf[nextin] = item
        print(f'Producer {n} produced {item} in slot {nextin}')
        nextin = (nextin + 1) % BUFSIZE
        mutex.release()
        full.release()
        sleep(0.2 * random.random())


def consumer(n):
    global buf, nextout

    for _ in range(N):
        full.acquire()
        mutex.acquire()
        item = buf[nextout]
        print(f'Consumer {n} consumed {item} from slot {nextout}')
        nextout = (nextout + 1) % BUFSIZE
        mutex.release()
        empty.release()
        sleep(0.2 * random.random())


producers = [threading.Thread(target=producer, args=[n]) for n in range(N)]
consumers = [threading.Thread(target=consumer, args=[n]) for n in range(2 * N)]

for t in producers + consumers:
    t.start()

for t in producers + consumers:
    t.join()
