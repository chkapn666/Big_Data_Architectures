import threading
from time import sleep
import random

NITEMS = 30

BUFSIZE = 10
buffer = [-1] * BUFSIZE
nextin = 0
nextout = 0

# A way to completely solve this issue is via using semaphores. Semaphores are also always linked to a lock, so we first define a lock before defining the semaphore.
mutex = threading.Lock()
empty = threading.Semaphore(BUFSIZE)  # empty reaching 0 will mean that the buffer is empty
# + at the very beginning, all buffer slots are empty
full = threading.Semaphore(0)  # full reaching BUFSIZE will mean that the buffer is full
# + at the very beginning, 0 buffer slots are full

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
