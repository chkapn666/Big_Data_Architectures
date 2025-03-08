import concurrent.futures
import time

start = time.perf_counter()

def do_job(seconds):
    print('Starting...')
    time.sleep(seconds)
    return f'Job finished {seconds}'

with concurrent.futures.ProcessPoolExecutor() as executor:
    secs = [5, 4, 3, 2, 1]
    results = executor.map(do_job, secs)

    for result in results:
        print(result)

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')