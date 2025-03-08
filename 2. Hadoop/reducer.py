#!/usr/bin/python
import sys

curr_word = None
curr_count = 0
# Process each key-value pair from the mapper
for line in sys.stdin:
    word, count = line.split('\t')  # Get the key and value from the current line
    count = int(count)  # convert count to int
    # If the current word is the same as the previous word, increment its count,
    # otherwise print the words count to stdout
    if word == curr_word:
        curr_count += count
    else:
        if curr_word:
            print('{0}\t{1}'.format(curr_word, curr_count))
        curr_word = word
        curr_count = count
if curr_word == word:
    print(f'{curr_word}\t{curr_count}')
