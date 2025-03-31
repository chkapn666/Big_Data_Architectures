import threading
from time import sleep
import random

NPROCESSES = 5  # equals # producers, (1/2) * # consumers, and # iterations that each thread will be delegated to complete

BUFSIZE = 10  # assumed size of the available printing queue
# The buffer needs to also retain the printing process' id for logging reasons
buf = [-1] * BUFSIZE
nextin = 0
nextout = 0

# The previous solution, making use only of threading.Lock synchronization primitives, was not enough to provide a satisfactory solution to our problem. 
# Even though locks could ensure the exclusive access to our shared resource, they are not capable to handle buffer fullness or emptiness. 
# A way to completely solve this issue is via using semaphores. Semaphores are also always linked to a lock, so we first define a lock before defining the semaphore.
# ===== Synchronization Primitives =====
mutex = threading.Lock()            # Protects buffer access (critical section; the buffer is an exclusive resource; no more than one thread should access it at a time)
empty = threading.Semaphore(BUFSIZE)  # Tracks EMPTY slots (starts full: BUFSIZE available)
# This means that when 'empty' is available for acquisition, processes that can lead to printing requirements have remaining room in the buffer to write at. 
# When it is not available / zero, then 'printing processes' should be blocked from producing any further, until at least one consumer consumers one value from the buffer. 
full = threading.Semaphore(0)        # Tracks FILLED slots (starts empty: 0 available)
# This means that when 'full' is available, then there are values in the buffer for the printer to consume & execute
# When it is not available / zero, the printer should be blocked from consuming any further, until at least one process writes one more value to the buffer.


def producer(n):
    global nextin
    global buf
    for i in range(NPROCESSES * 2):
        empty.acquire()  # blocks a producer when there is NO empty slot left, i.e. when there's nothing to consume
        
        mutex.acquire()  # ensuring exclusive access of a consumer thread to the shared resource only if there indeed exists sth for them to consume
        buf[nextin] = i  # the iteration 'i' for the printing job 'printing_process_id' is submitted to the printing queue
        print(f'Producer {n}: produce item {i} to buf')
        nextin = (nextin + 1) % BUFSIZE
        
        mutex.release()
        full.release()
        sleep(0.1 * random.random())


def consumer(n):
    global nextout
    global buf
    for i in range(NPROCESSES):  # the printer runs NITERS iterations PER PROCESSES - so NITERS * NPROCESSES in total
        full.acquire()  # blocks the consumer (=> makes him wait) when there is NO filled in slot

        mutex.acquire()  
        item = buf[nextout]
        print(f'Consumer {n}: consume item {item} from buf')
        nextout = (nextout + 1) % BUFSIZE

        mutex.release()  # stops the exclusion of other threads from the shared resource
        empty.release()  # adds +1 to the value of the semaphore controlling the filled-in slots
        sleep(0.1 * random.random())


producers = [threading.Thread(target=producer, args=[n]) for n in range(NPROCESSES)]
consumers = [threading.Thread(target=consumer, args=[n]) for n in range(2 * NPROCESSES)]

for t in producers + consumers:
    t.start()

for t in producers + consumers:
    t.join()
