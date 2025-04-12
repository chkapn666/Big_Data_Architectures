### Importing libraries and dependencies ###
import time  # needed to gauge the performance of our script (i.e. the time it takes to run)
import concurrent.futures


### Defining basic functions ### 
def calc_subranges(max_num, subranges_no):  
    """This function breaks down the range(max_num) to a specified number of sub-ranges"""
    step = max_num // subranges_no  # Step size
    subranges = [range(i * step, (i + 1) * step) for i in range(subranges_no)]

    return subranges  # returns a list with 'range' python objects as elements



def is_special(n):
    """This function checks whether a given number is special.
    A number is special if the sum of its digits is divisible by the number of its digits."""
    num_as_string = str(n)
    num_digits = len(num_as_string)
    sum_of_digits = sum([int(i) for i in num_as_string])
    if sum_of_digits % num_digits == 0:
        return True
    else:
        return False



def special_numbers_of_range(subrange):   
    """This function calculates the count of prime numbers in a given subrange""" 
    return sum(1 for n in subrange if is_special(n))  



### Main Function of Execution Runtime ###
def main(max_num, processes_to_run): 
    start = time.perf_counter()
    total_count_specials = 0  # This is a master counter; I break down the job of counting the primes in the entire range;
    # I work on multiple subranges concurrently, and I update the total counter at the same time.
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=processes_to_run) as executor:
        results = [executor.submit(special_numbers_of_range, calc_subranges(max_num, processes_to_run)[_]) 
                        for _ in range(processes_to_run)]  # so i'm running #(processes_to_run) processes with 
                        # a number of workers to be used at maximum exactly equal to that, leading the OS to 
                        # map each of the processes to a single worked
        
        for f in concurrent.futures.as_completed(results):
            total_count_specials += f.result()  # updating the total counter as soon as a result for a subrange has been produced
    
    elapsed = time.perf_counter() - start
    return elapsed, total_count_specials

if __name__ == "__main__":
    print("# Performance with different numbers of processes and different max_num values")
    print(f"#\tmax_num\t16\t8\t4\t(nspecials)")
    
    for max_num in [1_000, 10_000, 100_000, 1_000_000]:
        times = {}
        nspecials = None
        
        for processes_to_run in [16, 8, 4]:
            elapsed, count = main(max_num, processes_to_run)
            times[processes_to_run] = elapsed
            nspecials = count  # I get the same count for a given max_num, regardless of the number of processes I run
        
        print(f"#\t{max_num}\t{times[16]:.2f}\t{times[8]:.2f}\t{times[4]:.2f}\t{nspecials}")


### RESULTS ### 
# Performance with different numbers of processes and different max_num values
#       max_num 16      8       4       (nspecials)
#       1000    0.00    0.00    0.00    355
#       10000   0.01    0.01    0.01    2604
#       100000  0.11    0.11    0.11    20604
#       1000000 1.19    1.18    1.18    170595