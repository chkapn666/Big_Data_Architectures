import threading
from time import sleep
import random

NITERS = 30
NPROCESSES = 5

# My solution is an application of the classic bounded-buffer ("producer-consumer") problem, where
# - Producers produce items into a buffer.
# - Consumers consume items from the buffer. 
# - Mutual exclusion and full/empty signalling are required to ensure correctness. 
# Here, the buffer will be a list of tuples, identifying submitted printing jobs and the current iteration pending for them to complete.
# The lock ('mutex') ensures exclusive access to the shared variable ('buffer'). This prevents both threads from trying to modify
# 'buffer' at the same time, which could lead, for example, to the consumers consuming invalid/incomplete data (aka to print a job where there actually
# is none, which is represented by the '-1' values of the buffer).
# The semaphores are used to impose strict rules of inter-thread communication and coordination. This way, 
# we enforce the desired behavior, which is expressed in the following rules:
# - A printing job needs to be submitted before the printer can work on it.
# - If there is a very high printing workload (aka the jobs that processes are trying to submit is larger than the size 
# of the buffer that the printer can retain), then further job submissions should be blocked. 
# This way, these two synchronization primitives prevent 'race conditions' that could lead to 'state inconsistencies'.


# Here, we actually have multiple producers (aka processes submitting printing jobs), and only one consumer (the single printing machine
# which retains a single printing job queue). 
BUFSIZE = 10  # assumed size of the available printing queue
# The buffer will hold elements of the type (printing_process_id, current_iteration_of_this_process).
# This is good for logging & debugging reasons.
buffer = [-1] * BUFSIZE
nextin = 0  # this is the offset that will show were producers can add values to the buffer next. 
nextout = 0  # this is the offset that shows the printer/consumer where it can consume next from the buffer. 

# Making use only of threading.Lock synchronization primitives would not be enough to provide a satisfactory solution to our problem. 
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


def process(printing_process_id):
    global nextin
    global buffer
    for i in range(NITERS):
        empty.acquire()  # blocks a producer when there is NO empty slot left, i.e. when there's nothing to consume
        
        mutex.acquire()  # ensuring exclusive access of a consumer thread to the shared resource only if there indeed exists sth for them to consume
        buffer[nextin] = (printing_process_id, i)  # the iteration 'i' for the printing job 'printing_process_id' is submitted to the printing queue
        print(f'Printing job {printing_process_id}: submitted printing job iteration {i} request to slot {nextin}')
        nextin = (nextin + 1) % BUFSIZE
        
        mutex.release()
        
        full.release()
        sleep(0.1 * random.random())


def printer():
    global nextout
    global buffer
    for i in range(NITERS * NPROCESSES):  # the printer runs NITERS iterations PER PROCESSES - so NITERS * NPROCESSES in total
        full.acquire()  # blocks the consumer (=> makes him wait) when there is NO filled in slot

        mutex.acquire()  
        pn, iteration = buffer[nextout]
        print(f'Printer: Printed iteration {iteration} of printing job {pn}')
        nextout = (nextout + 1) % BUFSIZE

        mutex.release()  # stops the exclusion of other threads from the shared resource

        empty.release()  # adds +1 to the value of the semaphore controlling the filled-in slots
        sleep(0.1 * random.random())


### Defining the threads to execute ###
t1 = threading.Thread(target=printer)  # there is only one consumer thread running (identified by the single printer machine itself)
t1.start()

# There are multiple ('NPROCESSES') producer threads running
process_threads = []
for _ in range(NPROCESSES):
    t = threading.Thread(target=process, args=[_])  # the 'args' argument needs to take in a list as a value,
    # so here I am inserting to it a list with a single element.
    t.start()
    process_threads.append(t)


### Starting the concurrent execution of threads ###
t1.join()
for thread in process_threads:
    thread.join()
