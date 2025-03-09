import time 
import multiprocessing

# This fnc that is executed concurrently needs to be defined outside of the main
# function in order to be picklable!
def do_something(seconds):
    print(f'Sleeping {seconds} second(s)...')
    time.sleep(seconds)
    print('Done Sleeping...')



def main():
    start = time.perf_counter()

    processes = []
    for _ in range(10):
        # 'args' in multiprocessing.Process need to be picklable, in contrast to 
        # 'threading' requirements that do not necessitate this!
        process = multiprocessing.Process(target=do_something, args=[1.5])
        process.start()
        processes.append(process)

    # we cannot run a join inside of the previous loop, because it would wrap them inside of the 
    # main process without having the rest of the processes defined - this would make it equivalent 
    # to synchronous execution...
    for process in processes:
        process.join()

    finish = time.perf_counter()

    print(f'Finished in {round(finish-start, 2)} second(s).')


# If we do not explicitly define this, the 'multiprocessing' module is going to raise a RuntimeError.
if __name__ == "__main__":
    main()