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

# In this example, there is uncontrollable access to a shared resource; the balance. 
# Operations on this resource do not happen atomically. 
# This might lead to particular state inconsistencies: 
# - Race conditions: concurrent access might corrupt the 'balance' state. 
#       (a) The two 'joins' here are executed at a very rapid speed, almost together. 
#           They both alter the value of the 'balance' global variable at the very same time & in an uncontrolled manner, so the one that happens to finish 
#           executing second actually defines the result of the call. 
#       (b) If we also operated a 'withdraw' function, the withdrawers could be trying to access data before the depositors finished depositing => this 
#           could lead to the consumption of invalid/incomplete data
# I can see (a) through running the script multiple times -> I will probably get different results

t1.join()
t2.join()

print(f'Balance: {balance}')
