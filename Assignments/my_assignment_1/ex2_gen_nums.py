import random

def generate_numbers(filename, count=2**20):
    with open(filename, 'w') as file:
        for _ in range(count):
            file.write(f"{random.randint(-1000000, 1000000)}\n")

if __name__ == "__main__":
    files = ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt']
    for f in files:
        generate_numbers(f)
