#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 19:43:06 2025

@author: chkapsalis
"""

import sys

for line in sys.stdin:
    # Get the words in each line
    words = line.split()
    
    # Generate the count for each word
    for word in words:
        # Write the key-value pair to stdout to be processed by the reducer. 
        # The key is anything before the first tab character and the vaue is everything after te first tab character
        print('{0}\t{1}'.format(word, 1))
        
