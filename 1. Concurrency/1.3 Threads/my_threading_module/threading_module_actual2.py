"""Advancement: instantiating and spawning multiple threads
PLUS we pass in arguments to the Thread() object's instantiation"""

# Why use threading? -> to  achieve the speedup of our program via running various 
# processes simultaneously instead of "synchronously". 

import time 
import threading

start = time.perf_counter()

def do_something(seconds):
    print(f'Sleeping {seconds} second(s)...')
    time.sleep(seconds)
    print('Done Sleeping...')

# We need to run two separate loops - one to instantiate and spawn our threads, and 
# then one to join them with the main thread to make sure they complete before the main
# thread's execution is over => this requires a list of our threads to loop over
threads = []
for _ in range(10):
    t = threading.Thread(target=do_something, args=[_])  # the 'args' argument needs to take in a list as a value
    t.start()
    threads.append(t)

for thread in threads:
    thread.join()


finish = time.perf_counter()

print(f'Finished in {finish-start} seconds.')

# When something is performed synchronously, it does not take a lot of CPU calculations
# This operation is I/O bound - we're waiting a lot around => running concurrently with 
# threads gives the impression of running simultaneouly, but it's not - yet again, there is 
# "scaling out" advantage
# <> cpu-bound processes with a lot of data crunching => running in parallel
# is more appropriate