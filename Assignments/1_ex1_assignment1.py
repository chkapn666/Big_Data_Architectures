import concurrent.futures
import os
import math

# Step 1: Function to count prime numbers in a given range
def count_primes(subrange): 
    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(math.sqrt(n)) + 1):
            if n % i == 0:
                return False
        return True

    return sum(1 for n in subrange if is_prime(n))  # Count primes in the range


# Step 2: Main function for multiprocessing
def main(max_count=100_000, max_workers=os.cpu_count()):
    
    # Step 2.1: Define range partitioning
    subranges_no = max_workers  # Use CPU count
    step = max_count // subranges_no  # Step size
    subranges = [range(i * step, (i + 1) * step) for i in range(subranges_no)]

    # Step 2.2: Parallel Execution
    total = 0
    if __name__ == "__main__":  # Required for multiprocessing
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(count_primes, subranges)

        # Step 2.3: Summing up the total prime count
        total = sum(results)

    return total


# Run the script
if __name__ == "__main__":
    total = main()
    print(f'The total number of prime numbers is {total}.')
