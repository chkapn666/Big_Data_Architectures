import threading
from time import sleep
import random


log = []
lock = threading.Lock()


def do_transaction(jobname, tasknum, sleeptime):
    global log
    lock.acquire()
    for i in range(5):
        log.append((jobname, tasknum, i))
        sleep(sleeptime * random.random())
    lock.release()


def do_job(jobname, sleeptime):
    for i in range(10):
        do_transaction(jobname, i, sleeptime)
        sleep(0.1 * random.random())


t1 = threading.Thread(target=do_job, args=['Job1', 0.1])
t2 = threading.Thread(target=do_job, args=['Job2', 0.2])

t1.start()
t2.start()

t1.join()
t2.join()

for i in range(len(log)):
    print(f'Job: {log[i][0]}, transaction: {log[i][1]}, step: {log[i][2]}')
