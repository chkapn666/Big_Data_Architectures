####### Exercise 2 #######

import threading
import time
import random
import concurrent.futures
import multiprocessing
import math


def calc_minmax_of_file(file_path):  
    """This function performs the calculation of the min and max integer value of a single specified file."""
    min, max = float('inf'), float('-inf')
                       
    with open(file_path, 'r') as file:
        for line in file:
            # Each line ought to contain a single integer value. I need to perform exception handling to ensure this. 
            try:
                num_considered = int(line.lstrip())
                if num_considered < min:
                    min = num_considered
                elif num_considered > max:
                    max = num_considered
            except:
                raise ValueError(f"Found a non integer value in the file {file_path}")
  
    return min, max  # returns a single tuple containing the paired desired values 
    # per file, in the form: (minimum_integer, maximum_integer)



def main(files):
    """Main Function of Execution"""
    start = time.perf_counter()
    mins, maxs = [], []  # lists containing the min and max values found per file examined

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(calc_minmax_of_file, files)

        for min_val, max_val in results:
            mins.append(min_val)
            maxs.append(max_val)
         
    elapsed = time.perf_counter() - start
    return elapsed, mins, maxs

if __name__ == "__main__":
    # The user needs to make sure that they run this script in the same directory where the underlying 
    # text files are stored
    files = ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt'] 

    elapsed, mins, maxs = main(files)
    print(f"The total operation lasted for {elapsed} seconds.")
    for i in range(4):
        print(f"File {files[i]}: min -> {mins[i]}, max -> {maxs[i]}")

