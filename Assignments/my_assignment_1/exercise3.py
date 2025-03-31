import threading
from time import sleep
import random

NITEMS = 30
buffer = 0  # when 0, we need a 'pinger' to 'ping'; when 1, a 'ping' message is printed and awaits for a 'ponger' to print 'pong'


# ===== Synchronization Primitives =====
mutex = threading.Lock()            # Protects buffer access (critical section; the buffer is an exclusive resource; no more than one thread should access it at a time)
empty = threading.Semaphore(1)  # Tracks EMPTY slots, aka slots at the very beginning of execution or when there is a 'pong' message printed out (starts full: BUFSIZE available)
# This means that when 'empty' is available for acquisition, 'pingers' have remaining room in the buffer to write at. 
# When it is not available / zero, then 'pingers' should be blocked from producing any further, until at least one consumer consumers one value from the buffer. 
full = threading.Semaphore(0)        # Tracks FILLED slots (starts empty: 0 available)
# This means that when 'full' is available, then there are values in the buffer for 'pongers' to consume.
# When it is not available / zero, 'pongers' should be blocked from consuming any further, until at least one 'pinger' writes one more value to the buffer.

def pinger():
    global nextin
    global buffer
    for i in range(NITEMS):
        empty.acquire()  # blocks the producer (=> makes him wait) when there is NO empty slot - empty semaphore integer value equals to 0

        mutex.acquire()  # if a producer is able to produce, they will ensure exclusive access to the shared resource - the buffer => this is the only region we cover with our lock
        buffer = 1
        print("Ping", end=" ")
        mutex.release()  # stops the exclusion of other threads from the shared resource

        full.release()  # adds +1 to the value of the semaphore controlling the filled-in slots
        sleep(0.1 * random.random())

def ponger():
    global nextout
    global buffer
    for i in range(NITEMS):
        full.acquire()  # blocks a consumer when there is NO filled-in slot, i.e. when there's nothing to consume
        
        mutex.acquire()  # ensuring exclusive access of a consumer thread to the shared resource only if there indeed exists sth for them to consume
        buffer = 0
        print("Pong")
        mutex.release()
        
        empty.release()
        sleep(0.1 * random.random())


t1 = threading.Thread(target=pinger)
t2 = threading.Thread(target=ponger)
# Now these 2 threads run concurrently - producers trying to produce and consumers trying to consume
# However, each of these 2 actions respects the lock and the semaphore, so it takes place with the required logic

t1.start()
t2.start()

t1.join()
t2.join()
