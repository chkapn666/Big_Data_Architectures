#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 19:38:18 2025

@author: chkapsalis
"""

from mrjob.job import MRJob 

class MRWordFrequencyCount(MRJob):
    
    def mapper(self, _, line):
        # There will be 3 (key-pair) values produced per line we parse 
        # Pair #1: 
        yield "chars", len(line)  # produces a ("chars", xxx) pair, where the "chars" is a constant string value - a unique key for
        # this element that we want to aggregate (number of chars per line)
        # Pair #2:
        yield "words", len(line.split())
        # Pair #3:
        yield "lines", 1
        
        
    def reducer(self, key, values):
        yield key, sum(values)  # here we want the values assigned to each of the three keys to be
        # aggregated in a particular manner; else we could define logic "if key==... then elif ... else ..."

if __name__ == "__main__":
    MRWordFrequencyCount.run()