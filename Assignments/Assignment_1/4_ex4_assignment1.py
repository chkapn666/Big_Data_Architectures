import time 
import threading
import random

class PrinterJob:

    def __init__(self, N, M):
        self.jobs = N
        self.iters = [0] * N # iterations done for each job - M for each required to consider complete
        self.slots = threading.Semaphore(20)  # assume there are 20 slots available on a particular printing system at any given time

        self.lock = threading.Lock()  # only one printing iteration can take place at a given moment
        self.condition = threading.Condition(self.lock)  # based on can_perform_print_iteration function output



    def can_perform_print_iteration(self, print_job_id):  # id of the job
        return self.iters[print_job_id] < M and self.slots._value > 0  # there are pending iterations AND there are free slots on the machine



    def print_job(self, print_job_id):
        while self.iters[print_job_id] < M:  # the print job has not concluded yet
            with self.condition:
                # checking to see whether there is any free slot to perform an iteration
                while not self.can_perform_print_iteration(print_job_id): 
                    self.condition.wait()
                
                # capture a free slot
                self.slots.acquire()
                
                # perform the actions constituting a printing iteration
                print(f'Performed iteration {self.iters[print_job_id]} for print job {print_job_id}.')
                time.sleep(random.random())
                
                # release the semaphore 
                self.slots.release()

                # account for the fulfillment of this iteration
                self.iters[print_job_id] += 1
                
    
                



if __name__ == "__main__":
    N = 50  # print jobs
    M = 20  # iterations needed for each to conclude
    print_job_workload = PrinterJob(N, M)
    print_jobs = [] 

    for _ in range(N):
        print_job = threading.Thread(target=print_job_workload.print_job(_))
        print_jobs.append(print_job)
        print_job.start()

    for print_job in print_jobs:
        print_job.join()









    