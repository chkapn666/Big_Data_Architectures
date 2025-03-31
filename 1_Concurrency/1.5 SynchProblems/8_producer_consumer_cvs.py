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
empty = threading.Condition()  # Signals "buffer has space" (producer condition)
full = threading.Condition()  # Signals "buffer has items" (consumer condition)



def producer():
    global nextin
    global printb
    global num_in_buffer

    for i in range(NITEMS):

        with empty:  # equivalent to a 'while true' statement
            while num_in_buffer == BUFSIZE:  # when the buffer is full => wait (can't produce any more) => sb needs to take us out of this state
                empty.wait()
        
            mutex.acquire()  # should it be 'legal' for a producer to produce, they would attempt to do so
            printb[nextin] = i
            print(f'Producer: produced {i} in slot {nextin}')
            nextin = (nextin + 1) % BUFSIZE
            num_in_buffer += 1  # now we have one more slot of the buffer filled-in 
            mutex.release()
        
        with full:
            full.notify_all()  # after producing sth, make the consumers know that they can TRY TO (~ they will compete with each other) consume sth => 
            # get them (~ those waiting on an empty buffer) out of their wait state

        sleep(0.1 * random.random())

def consumer():
    global nextout
    global printb
    global num_in_buffer

    for i in range(NITEMS):
        with full:
            while num_in_buffer == 0:  # blocks the consumer from consuming when the buffer is empty
                full.wait()

            mutex.acquire()
            item = printb[nextout]
            print(f'Consumer: consumed {item} from slot {nextout}')
            nextout = (nextout + 1) % BUFSIZE
            num_in_buffer -= 1  # there's in one less filled-spot in the buffer!
            mutex.release()
        
        with empty:
            empty.notify_all()  # after consuming something, let the producers know that there is a spot for them to contribute to the buffer

        sleep(0.1 * random.random())


t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)

t1.start()
t2.start()

t1.join()
t2.join()
