####### Exercise 1 #######


### Importing libraries and dependencies ###
import threading
import time
import random
import concurrent.futures
import multiprocessing
import math


### Defining basic functions ### 
def calc_subranges(max_num, subranges_no):  
    """This function breaks down the range(max_num) to a specified number of sub-ranges"""
    # Step 2.1: Define range partitioning
    step = max_num // subranges_no  # Step size
    subranges = [range(i * step, (i + 1) * step) for i in range(subranges_no)]

    return subranges  # returns a list with 'range' python objects as elements



def is_perfect(n):
    """This function checks whether a given number is a perfect number."""
    if n < 2:
        return False
    divisors = [1]
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            divisors.append(i)
            if i != n // i:
                divisors.append(n // i)
    return sum(divisors) == n



def perfects_of_range(subrange):   
    """This function calculates the count of perfect numbers in a given subrange.""" 
    return sum(1 for n in subrange if is_perfect(n))  



### Main Function of Execution Runtime ###
def main(max_num, processes_to_run):  # The assignment asks for 
    start = time.perf_counter()
    total_count_perfects = 0  # This is a master counter; I break down the job of counting the primes in the entire range;
    # I work on multiple subranges concurrently, and I update the total counter at the same time.
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=processes_to_run) as executor:
        results = [executor.submit(perfects_of_range, calc_subranges(max_num, processes_to_run)[_]) 
                        for _ in range(processes_to_run)]  # so i'm running #(processes_to_run) processes with 
                        # a number of workers to be used at maximum exactly equal to that, leading the OS to 
                        # map each of the processes to a single worked
        
        for f in concurrent.futures.as_completed(results):
            total_count_perfects += f.result()  # updating the total counter as soon as a result for a subrange has been produced
    
    elapsed = time.perf_counter() - start
    return elapsed, total_count_perfects

if __name__ == "__main__":
    print("# Performance with linear ranges")
    print(f"#\t16\t8\t4\t(nperfects)")
    
    for max_num in [1000, 10000, 100000, 1000000, 10000000, 100000000]:
        times = {}
        nperfects = None
        
        for processes_to_run in [16, 8, 4]:
            elapsed, count = main(max_num, processes_to_run)
            times[processes_to_run] = elapsed
            nperfects = count  # Same count regardless of process number
        
        print(f"#\t{max_num}\t{times[16]:.2f}\t{times[8]:.2f}\t{times[4]:.2f}\t{nperfects}")
