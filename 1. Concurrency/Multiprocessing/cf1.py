import concurrent.futures
import time

start = time.perf_counter()

def do_job(seconds):
    print('Starting...')
    time.sleep(seconds)
    return 'Job finished'

with concurrent.futures.ThreadPoolExecutor() as executor:
    f1 = executor.submit(do_job, 1)
    f2 = executor.submit(do_job, 1)
    print(f1.result())
    print(f2.result())


finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')