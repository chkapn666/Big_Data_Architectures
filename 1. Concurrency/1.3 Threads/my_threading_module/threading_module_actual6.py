"""Advancement: introducing synchronization locks
    Introducing the Lock primitive - this solves the issue presented in the 
    previous script.
 """

import threading
import time



class ThreadCounter:
    
    def __init__(self):
        self.counter =0 
        self.lock = threading.Lock()  # we only define a single lock because there 
        # is a single exclusive resource to control access to 

    def count(self, thread_no):
        while True:
            # This is the section we need to lock - where the accessing of the unique resoucrce happens
            self.lock.acquire()
            # ~ Now, no two threads can do the following at the same time
            self.counter += 1 
            print(f"{thread_no}: Just increased counter to {self.counter}")
            time.sleep(1)  # this simulates the case where we're actually doing some work
            print(f"{thread_no}: Done some work, now value is {self.counter}")
            # now there's nothing to lock - we can allow other threads to work on it
            self.lock.release()


tc = ThreadCounter()

for _ in range(30):
    t = threading.Thread(target=tc.count, args=[_])  # the thread_no is used in ordeer 
    # to know which thread does what
    t.start()


