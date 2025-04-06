import threading
from time import sleep
import time
import random


log = []  # the `log` object requires exclusive handling by threads trying to access it in order to make sure that 
# the actions we try to document/"log" actually take place in an ordered and logical manner + they are documented in an
# easy to parse and understand manner.
# However, here it gets accessed in an uncontrollable way, so we see steps of transactions of the two jobs being intertwined, 
# and not executed in order.


def do_transaction(jobname, tasknum, sleeptime):
    global log
    for i in range(5):
        log.append((jobname, tasknum, i))
        sleep(sleeptime * random.random())


def do_job(jobname, sleeptime):
    for i in range(10):
        do_transaction(jobname, i, sleeptime)
        time.sleep(0.1 * random.random())


t1 = threading.Thread(target=do_job, args=['Job1', 0.1])
t2 = threading.Thread(target=do_job, args=['Job2', 0.2])

t1.start()
t2.start()

t1.join()
t2.join()

for i in range(len(log)):
    print(f'Job: {log[i][0]}, transaction: {log[i][1]}, step: {log[i][2]}')
