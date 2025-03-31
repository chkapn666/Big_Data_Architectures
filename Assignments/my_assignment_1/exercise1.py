####### Exercise 1 #######

import threading
import time
import random
import concurrent.futures
import multiprocessing
import math

# function breaking down the range(max_num) to a specified number of subranges
def calc_subranges(max_num, subranges_no):  # I use the cpu count as the default for the number of subranges to be checked to make sure that there will be exactly ony cpu core that each thread will be able to be assigned to
    # Step 2.1: Define range partitioning
    step = max_num // subranges_no  # Step size
    subranges = [range(i * step, (i + 1) * step) for i in range(subranges_no)]

    return subranges  # returns a list of 'range' python object 


# function checking whether a particular number is prime
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

# function counting how many numbers of a specified subrange are prime
def primes_of_range(subrange):    
    return sum(1 for n in subrange if is_prime(n))  

def main(max_num, processes_to_run):
    start = time.perf_counter()
    total_count_primes = 0
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=processes_to_run) as executor:
        results = [executor.submit(primes_of_range, calc_subranges(max_num, processes_to_run)[_]) 
                  for _ in range(processes_to_run)]
        
        for f in concurrent.futures.as_completed(results):
            total_count_primes += f.result()
    
    elapsed = time.perf_counter() - start
    return elapsed, total_count_primes

if __name__ == "__main__":
    print("# Performance with linear ranges")
    print(f"#\t\t\t16\t\t8\t\t4\t(nprimes)")
    
    for max_num in [1000, 10000, 100000, 1000000, 10000000, 100000000]:
        times = {}
        nprimes = None
        
        for processes_to_run in [16, 8, 4]:
            elapsed, count = main(max_num, processes_to_run)
            times[processes_to_run] = elapsed
            nprimes = count  # Same count regardless of process number
        
        print(f"#\t{max_num}\t{times[16]:.2f}\t{times[8]:.2f}\t{times[4]:.2f}\t{nprimes}")
