import threading
import time

start = time.perf_counter()

def do_job(n, seconds):
    print(f'Thread {n} Starting...')
    time.sleep(seconds)
    print(f'Job {n} finished')

threads = []
for i in range(10):
    t = threading.Thread(target=do_job, args=[i, 1.5])
    t.start()
    threads.append(t)

for thread in threads:
    thread.join()


finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')