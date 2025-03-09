"""Advancement: the Dining Philosophers' Problem"""

# 5 philosophers around the table, zero-indexed
# Each of them has their plates on the table
# There is also milk and chopsticks on the table
# 5 philosophers, 5 chopsticks, 5 plates
# A philosopher can be in 2 separate states:
    # A philosopher can think
    # A philosopher can eat
# At the beginning of the problem, all philosophers are thinking for a 
# particular amount of mine. 
# Then someone decides to eat. In order for a philosopher to eat, they need 
# to have 2 chopsticks. 
# At one time a philosopher may only take 1 chopstick
# We assume that all of them have the left chopstick, and at any time they are waiting
# to get the right chopstick.

# So the possibility for a'deadlock' is open - if they all tried to obtain the right chopstick at 
# once, no-one would achieve in doing so. 

# We will consider each meal (i.e. a philosopher eating) as a job to be completed
# and each philosopher as a process
# and the chopsticks as our finite resources 

import time 
import random
import threading 



class DiningPhilosophers_w_Locks:
    def __init__(self, total_phils_no=5, meal_size=7):  
        # total_phils_no: the number of philosophers on the table
        # meal_size: indicator of the size of the meal that philosophers have on their plate, i.e. how 
        # many times they need to 'eat' in order for them to finish their plate
        self.meals = [meal_size for _ in range(total_phils_no)]  # array w/ the meal on each philosopher's plate
        # each chopsticks only by one philosopher - we will implement a lock for each 
        self.chopsticks = [threading.Lock() for _ in range(total_phils_no)] # since there are as many chopsticks as there are philosophers

        self.status = ['  T  ' for _ in range(total_phils_no)]  # at the beginning, all philosophers are thinking - none is eating
        self.chopstick_holders = ['     ' for _ in range(total_phils_no)] # how many have both chopsticks on their hands at a given time

    # method used to instantiate and spawn our threads
    def philosopher(self, i):  # i: a unique ID for the philosopher considered at any given time
        while self.meals[i] > 0:  # i.e. if there need to be implemented more 'eating' processes
            
            self.status[i] = '  T  '
            time.sleep(random.random())  # representing the state where the philosopher is 'thinking'
            # about deciding to eat, i.e. attempting to obtain a right-hand side chopstick
            # we need to check the status of the left-hand chopstick of each philosopher, 
            # to make sure that it has not been already acquired by their left-side philosopher
            self.status[i] = '  _  '  # intermediate situation between thinking and eating; waiting to acquire two chopsticks


            if not self.chopsticks[i].locked():
                # if not locked, then MAYBE they could be able to eat
                self.chopsticks[i].acquire()
                self.chopstick_holders[i] = ' /   '
                time.sleep(random.random())

                # Phi 0: gets the chopstick 0 and attempts to get chopstick 1
                # ...
                # Phi 4: gets the chopstick 4 and attempts to get chopstick 0
                # This is why we do not use 'i+1' but '(i+1) % 5' so that the chopstick no does not go to 5, 
                # but instead it 'circles around' 0 to 5
                j = (i+1) % 5
                if not self.chopsticks[j].locked():  # they are free to acquire their right-hand chopstick and eat! et!
                    self.chopsticks[j].acquire()
                    self.chopstick_holders[i] = ' / \\ '  # representing both chopsticks being held
                    self.status[i] = '  E  '  # eating status
                    time.sleep(random.random())
                    # the philosopher eats so ...
                    self.meals[i] -= 1 
                    # now he has eaten so he should release both chopsticks
                    self.chopsticks[i].release()
                    self.chopstick_holders[i] = ' /   '
                    self.chopsticks[j].release()
                    self.chopstick_holders[i] = '     '
                
                else:
                    # since the philosopher cannot eat, they should release the other chopstick to allow for 
                    # the better flow of this process for the rest of the philosophers - prevent deadlocks!
                    self.chopsticks[i].release()
                    self.chopstick_holders[i] = '     '



class DiningPhilosophers_w_Semaphores:
    def __init__(self, total_phils_no=5, meal_size=7):  
        self.meals = [meal_size for _ in range(total_phils_no)]  # array w/ the meal on each philosopher's plate
        # the difference with the previous class lies on how the resource is handled - 
        # the equivalent of a lock is a semaphore with value=1 (only 1 philosopher can use 
        # the chopstick at a given time). So just changing this particular part of the 
        # chopstick creation will make the two implementations equivalent 
        self.chopsticks = [threading.Semaphore(value=1) for _ in range(total_phils_no)] # since there are as many chopsticks as there are philosophers

        self.status = ['  T  ' for _ in range(total_phils_no)]  # at the beginning, all philosophers are thinking - none is eating
        self.chopstick_holders = ['     ' for _ in range(total_phils_no)] # how many have both chopsticks on their hands at a given time

    # method used to instantiate and spawn our threads
    def philosopher(self, i):  # i: a unique ID for the philosopher considered at any given time
        while self.meals[i] > 0:  # i.e. if there need to be implemented more 'eating' processes
            
            self.status[i] = '  T  '
            time.sleep(random.random())  # representing the state where the philosopher is 'thinking'
            # about deciding to eat, i.e. attempting to obtain a right-hand side chopstick
            # we need to check the status of the left-hand chopstick of each philosopher, 
            # to make sure that it has not been already acquired by their left-side philosopher
            self.status[i] = '  _  '  # intermediate situation between thinking and eating; waiting to acquire two chopsticks


            if self.chopsticks[i].acquire(timeout=1):
                # keep trying to acquire this resource for only 1 second! After this, just 
                # just go without it (i.e. it leads us to the 'else' clause) - if we manage to acquire it
                # then this is equivalent to having acquired the lock - to the lock not being locked
                self.chopstick_holders[i] = ' /   '
                time.sleep(random.random())

                # Phi 0: gets the chopstick 0 and attempts to get chopstick 1
                # ...
                # Phi 4: gets the chopstick 4 and attempts to get chopstick 0
                # This is why we do not use 'i+1' but '(i+1) % 5' so that the chopstick no does not go to 5, 
                # but instead it 'circles around' 0 to 5
                j = (i+1) % 5
                if self.chopsticks[j].acquire(timeout=1):
                    self.chopstick_holders[i] = ' / \\ '  # representing both chopsticks being held
                    self.status[i] = '  E  '  # eating status
                    time.sleep(random.random())
                    # the philosopher eats so ...
                    self.meals[i] -= 1 
                    # now he has eaten so he should release both chopsticks
                    self.chopsticks[i].release()
                    self.chopstick_holders[i] = ' /   '
                    self.chopsticks[j].release()
                    self.chopstick_holders[i] = '     '
                
                else:
                    # since the philosopher cannot eat, they should release the other chopstick to allow for 
                    # the better flow of this process for the rest of the philosophers - prevent deadlocks!
                    self.chopsticks[i].release()
                    self.chopstick_holders[i] = '     '



class DiningPhilosophers_w_ConditionVariables:

    def __init__(self, total_phils_no=5, meal_size=7):
        self.total_phils_no = total_phils_no
        self.meals = [meal_size for _ in range(self.total_phils_no)]
        self.chopsticks = [False] * self.total_phils_no  # Boolean list indicating if a chopstick is in 
        # use - at the beginning, all philosophers are 'thinking', so none is in use

        self.lock = threading.Lock()  # a condition variable is always used combined with a lock
        # here we have only a single lock for accessing shared resources - now, the resources to which we 
        # require exclusive access is just the 'chopsticks' list
        self.condition = threading.Condition(self.lock)  # conditional variable for synchronization

        self.status = ['  T  ' for _ in range(self.total_phils_no)]  # Thinking status
        self.chopstick_holders = ['     ' for _ in range(self.total_phils_no)]  # Who holds chopsticks?


    # condition to be used in combination with the conditional variable !!!
    def can_eat(self, i):
        """ Check if a philosopher can eat (i.e., both his left AND right chopsticks are free) """
        left = i
        right = (i+1) % self.total_phils_no
        return not self.chopsticks[right] and not self.chopsticks[left]
    

    def philosopher(self, i):
        while self.meals[i] > 0:  # the philosopher has not finished eating so he's in the lookout for chopsticks to do so
            self.status[i] = '  T  ' # starting from a thinking phase
            time.sleep(random.random())

            self.status[i] = '  _  '  # on the lookout to acquire chopsticks

            with self.condition:
                while not self.can_eat(i):  # wait until both chopsticks become available 
                    self.condition.wait()  # wait until it gets notified to see whether then both chopstics have become available
                    # up to then, it will not touch anything
                
                # acquiring chopsticks
                left = i
                right = (i+1) % self.total_phils_no
                # here we're working with the exclusive resource so we ought to use the lock
                # self.lock.acquire()
                self.chopsticks[left] = True
                self.chopsticks[right] = True

                self.chopstick_holders[i] = ' / \\ '
                self.status[i] = '  E  '  # eating
                time.sleep(random.random())

                # Eating completed
                self.meals[i] -= 1

                # releasing chopsticks after eating
                self.chopsticks[left] = False
                self.chopsticks[right] = False
                # done working with the exclusive resource
                # self.lock.release()

                # Notify all waiting philosophers that a chopstick is now available !
                self.condition.notify_all()



def main_lock():
    n = 5 
    m = 9
    dining_philosophers_w_locks = DiningPhilosophers_w_Locks(n, m)
    philosophers = [] 
    for _ in range(n):
        philosopher = threading.Thread(target=dining_philosophers_w_locks.philosopher, args=[_])
        philosopher.start()
        philosophers.append(philosopher)

    ### this part could be skipped - we're using it for debugging and presentational purposes ###
    while sum(dining_philosophers_w_locks.meals) > 0:
        print("=" * (n*5))
        print("".join(map(str, dining_philosophers_w_locks.status)), 
                " : ", 
                str(dining_philosophers_w_locks.status.count('  E  '))
            )
        print("". join(map(str, dining_philosophers_w_locks.chopstick_holders)))
        print(dining_philosophers_w_locks.meals)
        time.sleep(0.1)
    ### this part could be skipped - we're using it for debugging and presentational purposes ###

    for _ in range(n):
        philosopher.join()



def main_semaphore():
    n = 5 
    m = 9
    dining_philosophers_w_semaphores = DiningPhilosophers_w_Semaphores(n, m)
    philosophers = [] 
    for _ in range(n):
        philosopher = threading.Thread(target=dining_philosophers_w_semaphores.philosopher, args=[_])
        philosopher.start()
        philosophers.append(philosopher)

    ### this part could be skipped - we're using it for debugging and presentational purposes ###
    while sum(dining_philosophers_w_semaphores.meals) > 0:
        print("=" * (n*5))
        print("".join(map(str, dining_philosophers_w_semaphores.status)), 
                " : ", 
                str(dining_philosophers_w_semaphores.status.count('  E  '))
            )
        print("". join(map(str, dining_philosophers_w_semaphores.chopstick_holders)))
        print(dining_philosophers_w_semaphores.meals)
        time.sleep(0.1)
    ### this part could be skipped - we're using it for debugging and presentational purposes ###

    for _ in range(n):
        philosopher.join()


def main_cv():
    n = 5 
    m = 9

    dining_philosophers_w_cv = DiningPhilosophers_w_ConditionVariables(n, m)
    philosophers = []

    for _ in range(n):
        philosopher = threading.Thread(target=dining_philosophers_w_cv.philosopher, args=[_])
        philosophers.append(philosopher)
        philosopher.start()

    ### this part could be skipped - we're using it for debugging and presentational purposes ###
    while sum(dining_philosophers_w_cv.meals) > 0:
        print("=" * (n * 5))
        print("".join(map(str, dining_philosophers_w_cv.status)), 
                " : ", 
                str(dining_philosophers_w_cv.status.count('  E  '))
            )
        print("".join(map(str, dining_philosophers_w_cv.chopstick_holders)))
        print(dining_philosophers_w_cv.meals)
        time.sleep(0.1)
    ### this part could be skipped - we're using it for debugging and presentational purposes ###


    for philosopher in philosophers:
        philosopher.join()



if __name__ == '__main__':
    #main_lock()
    #main_semaphore()
    main_cv()


