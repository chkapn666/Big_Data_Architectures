import concurrent.futures
import time

start = time.perf_counter()

def do_job(seconds):
    print('Starting...')
    time.sleep(seconds)
    return f'Job finished {seconds}'

with concurrent.futures.ThreadPoolExecutor() as executor:
    secs = [5, 4, 3, 2, 1]
    results = [executor.submit(do_job, sec) for sec in secs]

    for f in concurrent.futures.as_completed(results):
        print(f.result())

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')