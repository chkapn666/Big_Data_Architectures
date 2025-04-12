import threading
from time import sleep
import random

# In my solution, I am going to utilize condition variables to tackle this challenge. 
# I will implement two condition variables, each of which will represent the awakening factor (aka the state 
# that allows a thread to execute its core function) for the 'pinger' and the 'ponger', separately. 
# Using these two condition variables will lead to retaining two different queues. 

N = 15
flag = 0  # when 0, we need a 'pinger' to 'ping'; when 1, a 'ping' message is printed and awaits for a 'ponger' to print 'pong'


# Together with condition variables, we are also always using one lock (mutex) to protect access to the shared flag (access to which needs to be exclusive):
# - One condition variable for each type of thread, so we wake up only the appropriate one (no wasted context switches).
# This improves performance and aligns with best practices in concurrent programming — threads should only be notified when they can safely proceed.
# The logic still enforces:
# - No 'Ping' unless the previous 'Pong' is done.
# - No 'Pong' unless there is a new 'Ping' first.

### Setting up the synchronization primitives to be used ###
lock = threading.Lock()                   # Lock to ensure mutual exclusion over the shared variable (flag)
cond_ping = threading.Condition(lock)     # Used to wake up 'pinger' threads when a 'pong' has been printed
cond_pong = threading.Condition(lock)     # Used to wake up 'ponger' threads when a 'ping' has been printed


def ping():
    # Each 'ping' process needs to execute its logic N times so that it concludes
    global flag
    for i in range(N):
        with lock:
            while flag != 0:
                # If flag is not empty (i.e., last 'Ping' not yet followed by a 'Pong'), we wait
                cond_ping.wait()

            flag = 1
            print("Ping", end=" ")  # produce a value (print "Ping")

            # After printing 'ping', we signal the 'ponger' thread that a new value is available to consume
            cond_pong.notify()

        sleep(0.1 * random.random())  # random pause to imitate real work being done


def pong():
    # Each 'pong' process also needs to execute its logic N times
    global flag
    for i in range(N):
        with lock:
            while flag != 1:
                # If flag is not full (i.e., no new 'Ping' yet), we wait
                cond_pong.wait()

            flag = 0
            print("Pong")  # consume the value (print "Pong")

            # Signal the 'pinger' thread that the flag is now free for a new 'Ping'
            cond_ping.notify()

        sleep(0.1 * random.random())


# Create and start threads
t1 = threading.Thread(target=ping)
t2 = threading.Thread(target=pong)

# Now these 2 threads run concurrently - producers trying to produce and consumers trying to consume
# However, each of these 2 actions respects the lock and the condition variables, so it takes place with the required logic

t1.start()
t2.start()

t1.join()
t2.join()


### RESULTS ###
## For N = 15, I get 15 "Ping Pong", which is correct
# >>> (base) ChristoorossAir:exam chkapsalis$ python Q1_ping_pong.py 
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong
# Ping Pong