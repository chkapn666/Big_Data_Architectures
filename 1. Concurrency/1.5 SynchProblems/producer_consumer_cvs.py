import threading
from time import sleep
import random

NITEMS = 30

BUFSIZE = 10
printb = [-1] * BUFSIZE
nextin = 0
nextout = 0
num_in_buffer = 0  # this is a counter of the filled-in slots of the buffer


# We can also solve this using conditional variables
# With each condition variable, python associates a lock 
# There's one exlusive-access resource here so we will use a single lock, 'mutex'
mutex = threading.Lock()
full = threading.Condition()
#full_lock = threading.Lock()
empty = threading.Condition()
#empty_lock = threading.Lock()


def producer():
    global nextin
    global printb
    global num_in_buffer

    for i in range(NITEMS):
        with full:  # equivalent to a 'while true' statement
            while num_in_buffer == BUFSIZE:  # when the buffer is full => wait (can't produce any more) => sb needs to take us out of this state
                full.wait()
            mutex.acquire()  # should it be 'legal' for a producer to produce, they would attempt to do so
            printb[nextin] = i
            print(f'Producer: produced {i} in slot {nextin}')
            nextin = (nextin + 1) % BUFSIZE
            num_in_buffer += 1  # now we have one more slot of the buffer filled-in 
            mutex.release()
        with empty:
            empty.notify_all()  # after producing sth, make the consumers know that they can TRY TO (~ they will compete with each other) consume sth => 
            # get them (~ those waiting on an empty buffer) out of their wait state

        sleep(0.1 * random.random())

def consumer():
    global nextout
    global printb
    global num_in_buffer

    for i in range(NITEMS):
        with empty:
            while num_in_buffer == 0:  # blocks the consumer from consuming when the buffer is empty
                empty.wait()
            mutex.acquire()
            item = printb[nextout]
            print(f'Consumer: consumed {item} from slot {nextout}')
            nextout = (nextout + 1) % BUFSIZE
            num_in_buffer -= 1  # there's in one less filled-spot in the buffer!
            mutex.release()
        with full:
            full.notify_all()

        sleep(0.1 * random.random())


t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)

t1.start()
t2.start()

t1.join()
t2.join()
