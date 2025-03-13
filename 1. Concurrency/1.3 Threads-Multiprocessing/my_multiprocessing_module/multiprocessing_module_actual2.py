"""Advancement: introducing the ProcessPoolExecutor of the 'concurrent.futures' module 
to expand the true concurrency execution of python code"""

import time 
import concurrent.futures

# This fnc that is executed concurrently needs to be defined outside of the main
# function in order to be picklable!
def do_something(seconds):
    print(f'Sleeping {seconds} second(s)...')
    time.sleep(seconds)
    return f'Done Sleeping {seconds} second(s)...'



def main():
    start = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        #f1 = executor.submit(do_something, 1)
        #print(f1.result())  # printing out the results - this will wait until the execution of all function calls completes

        # scheduling the function to be executed one at a time for multiple times
        results = [executor.submit(do_something, (_ % len([5,4,3,2,1])) + 1) for _ in range(10)]

        # fetching the results as they are concurrently computed:
        for f in concurrent.futures.as_completed(results):
            # TODO: if we needed to handle any exceptions, we would do it here, 
            # working on the 'Future' object
            print(f.result())  # each f, i.e. each element of the 'results' list, 
            # is a 'Future' object


    # else i could get results in the same order as they have been inputted
    with concurrent.futures.ProcessPoolExecutor() as executor:
        secs = [5,4,3,2,1]
        results = executor.map(do_something, secs)  
        # each result, i.e. each element of the 'results' list, 
        # is an actual result with the actual result data type

        for result in results:
            # TODO: if we were to check for any exceptions, we would do it here,
            # inside of the results list
            print(result)
        

    finish = time.perf_counter()

    print(f'Finished in {round(finish-start, 2)} second(s).')


# If we do not explicitly define this, the 'multiprocessing' module is going to raise a RuntimeError.
if __name__ == "__main__":
    main()