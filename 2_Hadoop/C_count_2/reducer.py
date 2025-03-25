#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 19:43:13 2025

@author: chkapsalis
"""

import sys

curr_word = None 
curr_count = 0 

# Process each key-value pair, as PRINTED OUT by the mapper.py
for line in sys.stdin:  # reads mapper output line-by-line
    word, count = line.split('\t')  # Get the key and value from the current line - it has to do with how the mapper PRINTS results
    count = int(count)  # convert count to int (it is initially parsed as a string value)

    # If the current word is the same as the previous word (aka "we're still on the same word"), increment its count
    # Hadoop Streaming AUTOMATICALLY SORTS AND GROUPS the mapper output by key (word), and passes those GROUPS to the reducer
    # So for "hello word hello" => mapper returns "hello    1" "world   1" "hello   1"
    # and reducer hets "hello   1" "hello   1" "world   1"
    if word == curr_word:
        curr_count += count 
    else:  # otherwise (aka if its a new word), print the current word's total and start a new count 
        if curr_word:  # i.e. it is not None
            print('{0}\t{1}'.format(curr_word, curr_count))
        curr_word = word
        curr_count = count 


# We also need to make sure to print the LAST word AFTER the loop finishes !!! 
if curr_word == word:
    print(f'{curr_word}\t{curr_count}')
    

# so the reducer aggregates and outputs "hello  2" "world   1"