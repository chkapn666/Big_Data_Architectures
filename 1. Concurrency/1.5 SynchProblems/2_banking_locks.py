import threading
from time import sleep
import random

balance = 1000
lock = threading.Lock()  # I will add a lock to the resource that I require exclusive access to - the balance
# thus, when one of the threads tries to alter its value, the other one won't be able to do the same
# This solves the problem described in the 'banking.py' - no matter how many times I run the algo, I get the very same results

def deposit(amount):
    global balance
    lock.acquire()
    tmp = balance
    sleep(0.1 * random.random())
    tmp = tmp + amount
    sleep(0.1 * random.random())
    balance = tmp
    lock.release()

t1 = threading.Thread(target=deposit, args=[100])
t2 = threading.Thread(target=deposit, args=[200])

t1.start()
t2.start()

t1.join()
t2.join()

print(f'Balance: {balance}')
