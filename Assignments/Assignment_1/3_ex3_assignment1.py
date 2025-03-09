import time 
import threading
import concurrent.futures
import multiprocessing 

# REMEMBER !!! NEVER ACTUALLY USE THE LOCK WHEN WORKING WITH CONDITIONAL VARIABLES !!! 

NITEMS = 50  # the no of times the 'ping' 'pong' pairs are supposed to show up
shared_buffer = True  # I initiate it as True bc i want the 'ping' to take the lead 
mutex = threading.Lock()   # a single lock ensuring exclusive access to the common 
# resource 'shared_buffer' - when it is True, then i can 'pong' - else i need to 'ping' to turn it to True

can_ping = threading.Condition(mutex)
can_pong = threading.Condition(mutex)


def ping():
    global shared_buffer

    for iter in range(NITEMS):
        with can_ping:
            if not shared_buffer == True:
                can_ping.wait()
            
            print(f'iter {iter},', 'ping')
            shared_buffer = False

            can_pong.notify_all()


def pong():
    global shared_buffer 

    for iter in range(NITEMS):
        with can_pong:
            if not shared_buffer == False:
                can_pong.wait()
            
            print(f'iter {iter},', 'pong')
            shared_buffer = True

            can_ping.notify_all()


            
t_ping = threading.Thread(target=ping)
t_pong = threading.Thread(target=pong)

t_ping.start()
t_pong.start()

t_ping.join()
t_pong.join()