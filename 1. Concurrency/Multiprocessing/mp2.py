import multiprocessing
import time

start = time.perf_counter()

def do_job():
    print('Starting...')
    time.sleep(1)
    print('Job finished')

processes = []
for _ in range(10):
    p = multiprocessing.Process(target=do_job)
    p.start()
    processes.append(p)

for process in processes:
    process.join()

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')