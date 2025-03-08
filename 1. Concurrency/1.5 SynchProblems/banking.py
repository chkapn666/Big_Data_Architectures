import threading
from time import sleep
import random

balance = 1000

def deposit(amount):
    global balance  # so that our local (aka in-function) calls to the balance object will refer to and globally alter its value
    tmp = balance
    sleep(0.1 * random.random())
    tmp = tmp + amount
    sleep(0.1 * random.random())
    balance = tmp

t1 = threading.Thread(target=deposit, args=[100])
t2 = threading.Thread(target=deposit, args=[200])

t1.start()
t2.start()

# the two 'joins' are executed at a very rapid speed - almost together => they both alter the value of 
# the 'balance' global variable at the very same time & in an uncontrolled manner, so the one that HAPPENS TO 
# finish executing second actually defines the result of the call
# thus, if I run this multiple times, i will probably get different results
t1.join()
t2.join()

print(f'Balance: {balance}')
