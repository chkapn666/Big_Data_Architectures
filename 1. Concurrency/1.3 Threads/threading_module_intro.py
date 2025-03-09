# Why use threading? -> to  achieve the speedup of our program via running various 
# processes simultaneously instead of "synchronously". 

import time 

start = time.perf_counter()

def do_something():
    print('Sleeping 1 second')
    time.sleep(1)
    print('Done Sleeping...')

# serially execute the following:
# sleep once for 1 sec
do_something()
# after this sec is over, sleep once more 
do_something()

finish = time.perf_counter()

print(f'Finished in {finish-start} seconds.')

# When something is performed synchronously, it does not take a lot of CPU calculations
# This operation is I/O bound - we're waiting a lot around => running concurrently with 
# threads gives the impression of running simultaneouly, but it's not - yet again, there is 
# "scaling out" advantage
# <> cpu-bound processes with a lot of data crunching => running in parallel
# is more appropriate