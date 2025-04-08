import threading
from time import sleep
import random

# In this solution, we are going to utilize condition variables to tackle this challenge. 
# We will implement two condition variables, each of which will represent the awakening factor (aka the state 
# that allows a thread to execute its core function) for the pingers and the pongers, separately. 
# Using these two condition variables will lead to retaining two different queues. 

NITEMS = 30
buffer = 0  # when 0, we need a 'pinger' to 'ping'; when 1, a 'ping' message is printed and awaits for a 'ponger' to print 'pong'



# We are using one lock (mutex) to protect access to the shared buffer and two condition variables:
# - One condition variable for each type of thread, so we can wake up only the appropriate one (no wasted context switches).
# This improves performance and aligns with best practices in concurrent programming — threads should only be notified when they can safely proceed.
# The logic still enforces:
# - No 'Ping' unless the previous 'Pong' is done.
# - No 'Pong' unless there is a new 'Ping' first.

# ===== Synchronization Primitives =====
lock = threading.Lock()                   # Lock to ensure mutual exclusion over the shared variable (buffer)
cond_ping = threading.Condition(lock)     # Used to wake up 'pinger' threads when a 'pong' has been printed
cond_pong = threading.Condition(lock)     # Used to wake up 'ponger' threads when a 'ping' has been printed


def pinger():
    # Each 'pinger' (producer) process needs to execute its logic NITEMS times
    global buffer
    for i in range(NITEMS):
        with lock:
            while buffer != 0:
                # If buffer is not empty (i.e., last 'Ping' not yet followed by a 'Pong'), we wait
                cond_ping.wait()

            buffer = 1
            print("Ping", end=" ")  # produce a value (print "Ping")

            # Signal the 'ponger' thread that a new value is available to consume
            cond_pong.notify()

        sleep(0.1 * random.random())  # random pause to imitate real work being done


def ponger():
    # Each 'ponger' (consumer) process also needs to execute its logic NITEMS times
    global buffer
    for i in range(NITEMS):
        with lock:
            while buffer != 1:
                # If buffer is not full (i.e., no new 'Ping' yet), we wait
                cond_pong.wait()

            buffer = 0
            print("Pong")  # consume the value (print "Pong")

            # Signal the 'pinger' thread that the buffer is now free for a new 'Ping'
            cond_ping.notify()

        sleep(0.1 * random.random())


# Create and start threads
t1 = threading.Thread(target=pinger)
t2 = threading.Thread(target=ponger)

# Now these 2 threads run concurrently - producers trying to produce and consumers trying to consume
# However, each of these 2 actions respects the lock and the condition variables, so it takes place with the required logic

t1.start()
t2.start()

t1.join()
t2.join()
