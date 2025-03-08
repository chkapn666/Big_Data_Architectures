import time

start = time.perf_counter()

def do_job():
    print('Starting...')
    time.sleep(1)
    print('Job finished')

do_job()
do_job()
do_job()

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')