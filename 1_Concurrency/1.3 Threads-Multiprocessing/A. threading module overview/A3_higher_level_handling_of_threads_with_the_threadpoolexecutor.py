"""Introducing the threads pool executor"""

import time 
# import threading  
# this lies on a different module
import concurrent.futures

start = time.perf_counter()

def do_something(seconds):
    print(f'Sleeping {seconds} second(s)...')
    time.sleep(seconds)
    return 'Done Sleeping...'


"""
with concurrent.futures.ThreadPoolExecutor() as executor:
    # submit method - makes a method to be executed and returns a 'Future' object
    f1 = executor.submit(do_something, 1)  # (fnc_name, arguments)  - if i had multiple arguments, I would specify them using tuples
    # We can use the 'Future' value to see (a) if the thread is running, (b) if it has completed execution, or (c) if it is done producing a result
    f2 = executor.submit(do_something, 1)
    print('f1 result:', f1.result())  # it fetches the particular result once it is produced and prints it out
    print('f2 result:', f2.result())
"""

###########

# generalizing the previous to multiple threads to run
with concurrent.futures.ThreadPoolExecutor() as executor:
    secs = [5, 4, 3, 2, 1]
    # Ensure the range doesn't exceed the list length
    results = [executor.submit(do_something, secs[_ % len(secs)]) for _ in range(10)]
    # Fetching the multiple results in the order as_completed
    for f in concurrent.futures.as_completed(results):
        print(f.result())

###########

# alternative to using a for loop in order to 'submit' the threads
with concurrent.futures.ThreadPoolExecutor() as executor:
    secs = [5,4,3,2,1]
    results = executor.map(do_something, secs)  # it does NOT return 'future' objects; it returns 
    # actual results AFTER computing them (in the same order as the specified input arguments), and stores them in the order they started executing
    # so here we're waiting for the thread assigned the 5-sec sleep before printing the rest
    for result in results:
        print(result)

finish = time.perf_counter()
print(f'{start-finish} second(s) in total')