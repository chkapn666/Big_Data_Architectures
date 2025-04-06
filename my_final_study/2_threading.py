import time
import threading


def do_job():
    print('Starting...')
    time.sleep(1)
    print('Job Finished!')


def do_job_w_args(secs):
    print('Starting...')
    time.sleep(secs)
    print('Job Finished - Secs Arg Value:', secs)



def main():
    start = time.perf_counter()

    ### Case 1: Without threading
    #do_job()
    #do_job()


    ### Case 2: Simple case of threading - 2 threads
    # thread1 = threading.Thread(target=do_job)
    # thread2 = threading.Thread(target=do_job)

    # thread1.start()
    # thread2.start()

    # thread1.join()
    # thread2.join()


    ### Case 3: Generalizing to a higher number of threads
    # threads = []
    # for _ in range(10):
    #     thread = threading.Thread(target=do_job)
    #     threads.append(thread)
    #     thread.start()
    
    # for thread in threads:
    #     thread.join()


    ### Case 4: Passing arguments into the 'target' function
    threads = []
    for _ in range(10):
        thread = threading.Thread(target=do_job_w_args, args=[_])
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    finish = time.perf_counter()

    print('Finished in', round(finish - start, 2), 'second(s)')



if __name__ == "__main__":
    main()