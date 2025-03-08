import concurrent.futures
import time

start = time.perf_counter()

def do_job(seconds):
    print('Starting...')
    time.sleep(seconds)
    return f'Job finished {seconds}'

with concurrent.futures.ProcessPoolExecutor() as executor:
    f1 = executor.submit(do_job, 1)
    print(f1.result())

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')