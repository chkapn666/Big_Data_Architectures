"""Introduction to the high-level threading functionality"""

# Why use threading? -> to  achieve the speedup of our program via running various 
# processes simultaneously instead of "synchronously". 

import time 
import threading

start = time.perf_counter()

def do_something():
    print('Sleeping 1 second')
    time.sleep(1)
    print('Done Sleeping...')

# I will create 1 thread per i/o bound process i want to run
t1 = threading.Thread(target=do_something)  # this is the first thread object i run
t2 = threading.Thread(target=do_something)

# we need to use the 'start' method on each thread 
t1.start() 
t2.start()

# we also need to 'join' these 2 threads with the main thread to make sure that the latter
# waits for the former to complete before it completes 
t1.join()
t2.join()


finish = time.perf_counter()

print(f'Finished in {finish-start} seconds.')

# When something is performed synchronously, it typically does not make full use of the computational (CPU/GPU) resources of the local machine.
# The operation shown in the previous example is I/O bound - involves a lot of waiting around; it does not include a multitude 
# # of calculations for the CPU to perform => running 'asynchronously' using Python's threads gives the impression of running concurrently, 
# but we're actually not doing that - yet again, there is a "scaling out" advantage via the more efficient usage of our local machine's available computational resources
# <> cpu-bound processes with a lot of data crunching => actually running in parallel ('concurrently') is more appropriate.