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


# This is not enough! We're still seeing consumers consuming '-1' from the buffer; 
# What we have done is that we have protected concurrent access to the shared buffer / exclusive resource, 
# yet a lock is not enough to handle buffer fullness or emptiness! 
# Thus, the current logic lets the producer write to the buffer even if the buffer is full.
# Moreover, it lets the consumer read from the buffer even if the buffer is empty. 
