import concurrent.futures as cf
import time

def rsum(rng):
    sum = 0
    for n in range(rng[0], rng[1] + 1):
        sum += n
    return sum

max_num = 1000000000
n_thr = 4

args = []
idx = 0
for i in range(n_thr - 1):
    args.append((idx, idx + max_num // n_thr))
    idx += max_num // n_thr + 1
args.append((idx, max_num))

start = time.perf_counter()

with cf.ProcessPoolExecutor(max_workers=n_thr) as executor:
    results = executor.map(rsum, args)

r = sum(results)

end = time.perf_counter()

print(f'Sum = {r} ({round(end - start, 2)} second(s))')
