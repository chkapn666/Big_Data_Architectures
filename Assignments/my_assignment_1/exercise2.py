####### Exercise 1 #######

import threading
import time
import random
import concurrent.futures
import multiprocessing
import math

# function breaking down the range(max_num) to a specified number of subranges
def calc_minmax_of_file(file_path):  # I use the cpu count as the default for the number of subranges to be checked to make sure that there will be exactly ony cpu core that each thread will be able to be assigned to
    # Step 2.1: Define range partitioning
    min, max = float('inf'), float('-inf')
                       
    with open(file_path, 'r') as file:
        for line in file:
            try:
                num_considered = int(line.lstrip())
                if num_considered < min:
                    min = num_considered
                elif num_considered > max:
                    max = num_considered
            except:
                raise ValueError(f"Found a non integer value in the file {file_path}")
  
    return min, max

def main(files):

    start = time.perf_counter()
    mins, maxs = [], []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(calc_minmax_of_file, files)

        for min_val, max_val in results:
            mins.append(min_val)
            maxs.append(max_val)
         
    elapsed = time.perf_counter() - start
    return elapsed, mins, maxs

if __name__ == "__main__":
    files = ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt']

    elapsed, mins, maxs = main(files)
    print(f"The total operation lasted for {elapsed} seconds.")
    for i in range(4):
        print(f"File {files[i]}: min -> {mins[i]}, max -> {maxs[i]}")

