import time
import threading
import concurrent.futures
import multiprocessing 

## Objective
# Compute the min and max number out of all f1.txt, f2.txt, f3.txt, f4.txt files, each of which contain
# a single number per row, 1048576 in total for each file. 

## Strategy
# I will consider parts of each file, find the min and max no in them, and then combine the results

# Global vars
file_names = ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt']

# lists that will hold the mix and max vals found in each file (i.e. store all intermediate results)
mins = [float("inf")] * len(file_names)
maxs = [float("-inf")] * len(file_names)

# list holding the (overall minimum, overall maximum)
overalls = [float("inf"), float("-inf")]  # (min, max)



# defining a function to find the min and max vals in a given file
def minmax(file_id):
    with open(file_names[file_id], 'r') as f:
        for line in f.readlines():
            # I read the file line by line, and work progressively towards calc the min and max 
            try:
                num = int(line)
            except:
                raise Exception("Problematic file - needs to have exclusively one integer number per line.")

            if num < mins[file_id]:
                mins[file_id] = num
            elif num > maxs[file_id]:
                maxs[file_id] = num

    return (mins[file_id], maxs[file_id])
    


def main(max_workers):    

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = [executor.submit(minmax, _) for _ in range(len(file_names))]

        for f in concurrent.futures.as_completed(results):
            res_tuple = f.result() 
            if res_tuple[0] < overalls[0]:
                overalls[0] = res_tuple[0]
            elif res_tuple[1] > overalls[1]:
                overalls[1] = res_tuple[1]

    return f"Overall min: {overalls[0]}. Overall max: {overalls[1]}."



if __name__ == "__main__":
    start = time.perf_counter()

    res = main(max_workers=4)
    
    print(res)

    finish = time.perf_counter()

    print(f"Total computation time: {round(finish-start, 2)} second(s).")