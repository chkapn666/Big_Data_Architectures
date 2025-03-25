"""
Instantiating and spawning multiple threads
PLUS passing arguments to the Thread() object's instantiation'
"""

import time 
import threading

start = time.perf_counter()

def do_something(seconds):
    print(f'Sleeping {seconds} second(s)...')
    time.sleep(seconds)
    print('Done Sleeping...')

# We need to run two separate loops - one to instantiate and spawn our threads, and 
# then one to join them with the main thread to make sure they complete before the main
# thread's execution is over => this requires specifying a list of our threads to loop over
threads = []
for _ in range(10):
    t = threading.Thread(target=do_something, args=[_])  # the 'args' argument needs to take in a list as a value
    t.start()
    threads.append(t)

for thread in threads:
    thread.join()


finish = time.perf_counter()

print(f'Finished in {finish-start} seconds.')
