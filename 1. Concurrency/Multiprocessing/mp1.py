import multiprocessing
import time

start = time.perf_counter()

def do_job():
    print('Starting...')
    time.sleep(1)
    print('Job finished')

p1 = multiprocessing.Process(target=do_job)
p2 = multiprocessing.Process(target=do_job)

p1.start()
p2.start()

p1.join()
p2.join()

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')