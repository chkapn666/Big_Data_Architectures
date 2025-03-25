"""
Introducing synchronization locks
This will be our base case, where there are no locks.
Different threads are trying to access & modify the value of a common resource
in an uncontrolled manner - the counter should increase linearly step-by-step (of 1), 
but now without any synchronization mechanisms we do not control the 
order in which it is referenced and modified, and thus we get erroneous results.
"""

import threading
import time

class ThreadCounter:
    
    def __init__(self):
        self.counter = 0 
        # self.lock = threading.Lock()

    def count(self, thread_no):
        while True:
            self.counter += 1 
            print(f"{thread_no}: Just increased counter to {self.counter}")
            time.sleep(1)  # this simulates the case where we're actually doing some work
            print(f"{thread_no}: Done some work, now value is {self.counter}")


tc = ThreadCounter()

for _ in range(30):
    t = threading.Thread(target=tc.count, args=[_])  # the thread_no is used in order to know which thread does what
    # Here, without using locks to handle the order of execution, we see that threads act in an unordered manner - e.g. thread #22 acts before thread #20
    t.start()
