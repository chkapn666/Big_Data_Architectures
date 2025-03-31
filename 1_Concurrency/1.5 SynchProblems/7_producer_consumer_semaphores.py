import threading
from time import sleep
import random

NITEMS = 30

BUFSIZE = 10
buffer = [-1] * BUFSIZE
nextin = 0
nextout = 0

# The previous solution, making use only of threading.Lock synchronization primitives, was not enough to provide a satisfactory solution to our problem. 
# Even though locks could ensure the exclusive access to our shared resource, they are not capable to handle buffer fullness or emptiness. 
# A way to completely solve this issue is via using semaphores. Semaphores are also always linked to a lock, so we first define a lock before defining the semaphore.
# ===== Synchronization Primitives =====
mutex = threading.Lock()            # Protects buffer access (critical section; the buffer is an exclusive resource; no more than one thread should access it at a time)
empty = threading.Semaphore(BUFSIZE)  # Tracks EMPTY slots (starts full: BUFSIZE available)
# This means that when 'empty' is available for acquisition, producers have remaining room in the buffer to write at. 
# When it is not available / zero, then producers should be blocked from producing any further, until at least one consumer consumers one value from the buffer. 
full = threading.Semaphore(0)        # Tracks FILLED slots (starts empty: 0 available)
# This means that when 'full' is available, then there are values in the buffer for consumers to consume.
# When it is not available / zero, consumers should be blocked from consuming any further, until at least one producer writes one more value to the buffer.

def producer():
    global nextin
    global buffer
    for i in range(NITEMS):
        empty.acquire()  # blocks the producer (=> makes him wait) when there is NO empty slot - empty semaphore integer value equals to 0

        mutex.acquire()  # if a producer is able to produce, they will ensure exclusive access to the shared resource - the buffer => this is the only region we cover with our lock
        buffer[nextin] = i
        print(f'Producer: produced {i} in slot {nextin}')
        nextin = (nextin + 1) % BUFSIZE
        mutex.release()  # stops the exclusion of other threads from the shared resource

        full.release()  # adds +1 to the value of the semaphore controlling the filled-in slots
        sleep(0.1 * random.random())

def consumer():
    global nextout
    global buffer
    for i in range(NITEMS):
        full.acquire()  # blocks a consumer when there is NO filled-in slot, i.e. when there's nothing to consume
        
        mutex.acquire()  # ensuring exclusive access of a consumer thread to the shared resource only if there indeed exists sth for them to consume
        item = buffer[nextout]
        print(f'Consumer: consumed {item} from slot {nextout}')
        nextout = (nextout + 1) % BUFSIZE
        mutex.release()
        
        empty.release()
        sleep(0.1 * random.random())


t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)
# Now these 2 threads run concurrently - producers trying to produce and consumers trying to consume
# However, each of these 2 actions respects the lock and the semaphore, so it takes place with the required logic

t1.start()
t2.start()

t1.join()
t2.join()
