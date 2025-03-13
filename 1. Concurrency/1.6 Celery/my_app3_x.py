from my_app3 import rsum
import time 

max_num = 1_000_000_000
n_worker = 4 

args = []

# typical design pattern - paradigm for splitting a range into a pre-defined number of sub-ranges
idx = 0
for i in range(n_worker-1):
    args.append((idx, idx+max_num//n_worker))
    idx += max_num // n_worker + 1 
args.append((idx, max_num))

start = time.perf_counter()

results = [rsum.delay(args[i]) for i in range(n_worker)]

while not all([results[i].ready() for i in range(n_worker)]):
    pass

total = sum([r.result for r in results])

end = time.perf_counter()

print(f'Sum = {total} ({round(end - start, 2)} seconds(s))')