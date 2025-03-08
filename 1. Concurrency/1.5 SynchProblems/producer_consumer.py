import threading
from time import sleep
import random

NITEMS = 30

# Our buffer here is essentially a shared log of print jobs. It is 10-elements long, yet we have 30 items to add to it.
# Thus, we need to make the updating of the buffer cyclical => this takes place through the command in line 34.
BUFSIZE = 10
buffer = [-1] * BUFSIZE  # in this shared buffer, producers can produce items - there must exist space for them to do so
# at the same time, consumers can consume items - there must be data for them to consume
# plus, when the buffer is full => further producers should be blocked until some consumer consumes sth
# plus, when the buffer is empty => further consumers should be blocked until producers append a new print job to the shared buffer
# Without controlling the access to the buffer, we might see the previous rules being violated, e.g. by getting consumers that 
# consume '-1', i.e. try to consume from the empty buffer + the slots of the buffer are accessed in an unruly manner
nextin = 0
nextout = 0

def producer():
    global nextin
    global buffer
    for i in range(NITEMS):
        buffer[nextin] = i
        print(f'Producer: produced {i} in slot {nextin}')
        nextin = (nextin + 1) % BUFSIZE  # the position in the buffer where the next producer will add sth
        sleep(0.1 * random.random())

def consumer():
    global nextout
    global buffer
    for i in range(NITEMS):
        item = buffer[nextout]
        print(f'Consumer: consumed {item} from slot {nextout}')
        nextout = (nextout + 1) % BUFSIZE  # the position in the buffer where the next consumer will consume sth
        sleep(0.1 * random.random())


t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)

t1.start()
t2.start()

t1.join()
t2.join()
