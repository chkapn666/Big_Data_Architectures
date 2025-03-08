import threading
from time import sleep
import random

NITEMS = 30

BUFSIZE = 10
printb = [-1] * BUFSIZE
nextin = 0
nextout = 0
mutex = threading.Lock()

def producer():
    global nextin
    global printb
    for i in range(NITEMS):
        mutex.acquire()
        printb[nextin] = i
        print(f'Producer: produced {i} in slot {nextin}')
        nextin = (nextin + 1) % BUFSIZE
        mutex.release()
        sleep(0.1 * random.random())

def consumer():
    global nextout
    global printb
    for i in range(NITEMS):
        mutex.acquire()
        item = printb[nextout]
        print(f'Consumer: consumed {item} from slot {nextout}')
        nextout = (nextout + 1) % BUFSIZE
        mutex.release()
        sleep(0.1 * random.random())


t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)

t1.start()
t2.start()

t1.join()
t2.join()
