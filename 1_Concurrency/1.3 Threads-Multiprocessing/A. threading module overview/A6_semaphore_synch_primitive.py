"""Advancement: introducing the semaphore
 """

import threading
import time

# Now there is a common resource that does NOT require exclusive access of 
# a single thread; rather, multiple ones (but a limited number) can access it once
semaphore = threading.BoundedSemaphore(value=3)

def access(thread_number):  # it is good to retain a unique identifier per thread to know which one is trying to access our resource
    print(f"{thread_number} is trying to access.")
    semaphore.acquire()
    print(f"{thread_number} was granted access.")
    time.sleep(5)
    print(f"{thread_number} now releasing.")
    semaphore.release()

threads = []
for _ in range(10):
    thread = threading.Thread(target=access, args=[_])
    thread.start()

for thread in threads:
    thread.join()

# so now we see threads acting in an ordered manner - triads of threads (with adjacent IDs) only access the resource at once