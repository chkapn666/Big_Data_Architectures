#!/usr/bin/python
import sys

d = {}

curr_word = None
curr_count = 0
# Process each key-value pair from the mapper
for line in sys.stdin:
    word, count = line.split('\t')  # Get the key and value from the current line
    count = int(count)  # convert count to int
    # If the current word is the same as the previous word, increment its count,
    # otherwise print the words count to stdout
    if word not in d:
        d[word] = 0
    d[word] += 1

for (k, v) in d.items():
    print(f'{k}\t{v}')
