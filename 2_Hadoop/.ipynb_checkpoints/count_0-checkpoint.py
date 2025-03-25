#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 19:25:14 2025

@author: chkapsalis
"""

from mrjob.job import MRJob 

"""
data.txt:
    Satan Satan Satan
    Die Die 
    Fk 
"""


class MRWordCount(MRJob):
    # Upon consuming a text file, the runtime will automatically break it down to different elements,
    # each comprising a single line of the file. We do not care about the "id" of a line, so we skip 
    # the "key" values (all blank) from the (key-value) digested pair. 
    def mapper(self, _, line):
        for word in line.split():
            yield word, 1   # so we get pairs
            # (Satan, 1), (Satan, 1), (Satan, 1), (Die, 1), (Die, 1), (Fk, 1)
    
    # now the reducer will ingest the (key-value) pairs produced by the mapper
    # and combine them; (Satan, [1,1,1]), (Die, [1,1]), (Fk, [1])
    def reducer(self, word, counts):
        yield word, sum(counts)  # producing (key-value) pairs (Satan, 3), (Die, 2), (Fk, 1)

if __name__ == "__main__":
    MRWordCount.run()
        
        
