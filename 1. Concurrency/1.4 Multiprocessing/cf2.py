import concurrent.futures
import time

start = time.perf_counter()

def do_job(seconds):
    print('Starting...')
    time.sleep(seconds)
    return 'Job finished'

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = [executor.submit(do_job, 1) for _ in range(10)]

    for f in concurrent.futures.as_completed(results):
        print(f.result())

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')