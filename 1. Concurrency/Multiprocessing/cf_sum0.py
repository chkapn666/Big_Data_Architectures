import concurrent.futures as cf
import time

max_num = 1000000000

start = time.perf_counter()

sum = 0
for n in range(max_num + 1):
    sum += n

end = time.perf_counter()

print(f'Sum = {sum} ({round(end - start, 2)} second(s))')
