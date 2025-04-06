import time 
import multiprocessing 
import concurrent.futures

def do_job():
    print('Starting...')
    time.sleep(1)
    print('Job Finished')


def do_job_w_args(job_id, secs):
    #print('Starting...')
    time.sleep(secs)
    #print('Job Finished')

    return f'Finished job {job_id} in {secs} second(s)'


def main():
    start = time.perf_counter()

    ### Case 1 - simplistic setup 
    # p1 = multiprocessing.Process(target=do_job)
    # p2 = multiprocessing.Process(target=do_job)

    # p1.start()
    # p2.start()

    # p1.join()
    # p2.join()


    ### Case 2 - generalizing 
    # processes = [] 
    # for _ in range(10):
    #     p = multiprocessing.Process(target=do_job)
    #     p.start()
    #     processes.append(p)
    
    # for process in processes:
    #     process.join()  # since multiprocessing achieves actual concurrency through direct communication with the OS, there 
    #     # is higher communication overhead we need to incur to use it - so we see here that the job finishes in 1.11 seconds instead of 1.01 
    #     # that we see when using the threading module => we need to scale out so that the marginal benefit is larger than the marginal cost 


    ### Case 3 - higher level setup; concurrent futures - obtaining results as computed!!!
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     secs = [5,3,4,1,2]
    #     results = [executor.submit(do_job_w_args, _, secs[_ % len(secs)]) for _ in range(10)]

    #     for f in concurrent.futures.as_completed(results):
    #         print(f.result())

    ### Case 4 - higher level setup; concurrent futures - obtaining results as inputted
    with concurrent.futures.ThreadPoolExecutor() as executor:
        secs = [5,3,4,1,2]  # we need an input array with the same length as the jobs we actually need to submit; no 
        # [... for] functionality supported with the map() method 
        results = executor.map(do_job_w_args, range(len(secs)), secs)
        # so here we actually collect ALL results before proceeding ! 
        for result in results:
            print(result)

    finish = time.perf_counter()
    print(f'Finished in {round(finish-start,2)} second(s).')



if __name__ == "__main__":
    main()