"""Advancement: introduction of the high-level threading functionalirty"""

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
# waits for the former to complete because it completes 
t1.join()
t2.join()


finish = time.perf_counter()

print(f'Finished in {finish-start} seconds.')

# When something is performed synchronously, it does not take a lot of CPU calculations
# This operation is I/O bound - we're waiting a lot around => running concurrently with 
# threads gives the impression of running simultaneouly, but it's not - yet again, there is 
# "scaling out" advantage
# <> cpu-bound processes with a lot of data crunching => running in parallel
# is more appropriate