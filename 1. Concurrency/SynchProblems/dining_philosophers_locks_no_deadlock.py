import threading
from time import sleep
import random

NUM = 5
ITER = 10

chopsticks = [False for _ in range(NUM)]
# NOT locks = [threading.Lock()] * NUM
locks = [threading.Lock() for _ in range(NUM)]


def philosopher(pnum):
    print(f'Philosopher {pnum} starting')
    for i in range(ITER):
        if pnum == NUM - 1:
            print(f'Philosopher {pnum} getting chopstick {(pnum + 1) % NUM}')
            locks[(pnum + 1) % NUM].acquire()
            chopsticks[(pnum + 1) % NUM] = True
            print(f'Philosopher {pnum} getting chopstick {pnum}')
            locks[pnum].acquire()
            chopsticks[pnum] = True
        else:
            print(f'Philosopher {pnum} getting chopstick {pnum}')
            locks[pnum].acquire()
            chopsticks[pnum] = True
            print(f'Philosopher {pnum} getting chopstick {(pnum + 1) % NUM}')
            locks[(pnum + 1) % NUM].acquire()
            chopsticks[(pnum + 1) % NUM] = True

        print(f'Philosopher {pnum} eating')
        sleep(0.1 * random.random())

        print(f'Philosopher {pnum} dropping chopstick {pnum}')
        locks[pnum].release()
        chopsticks[pnum] = False
        print(f'Philosopher {pnum} dropping chopstick {(pnum + 1) % NUM}')
        locks[(pnum + 1) % NUM].release()
        chopsticks[(pnum + 1) % NUM] = False

        print(f'Philosopher {pnum} going to sleep')
        sleep(0.2 * random.random())

    print(f'Philosopher {pnum} terminating')

philosophers = [threading.Thread(target=philosopher, args=[pnum]) for pnum in range(NUM)]

for pnum in range(NUM):
    philosophers[pnum].start()

for pnum in range(NUM):
    philosophers[pnum].join()
