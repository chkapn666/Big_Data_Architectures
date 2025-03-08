import threading
import time

start = time.perf_counter()

def do_job():
    print('Starting...')
    time.sleep(1)
    print('Job finished')

t1 = threading.Thread(target=do_job)
t2 = threading.Thread(target=do_job)

t1.start()
t2.start()

#t1.join()
#t2.join()

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')