#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 20:48:39 2025

@author: chkapsalis
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):
    
    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)  # case-insensitive comparisons 
            # returns key-value pairs like (1st_word,1), (2nd_word,1), (1st_word,1), ... 
    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))  # calculating the total count for each word/key of the previously 
        # produced key-value pairs
    
    # In order to be able to compare multiple tuples, i need to them to have the same key -> None key
    # Moreover, by default comparisons are being made only based on the value of the first element of tuples, 
    # so I need to somehow rotate each tuple. The 'sum(counts)' expression here is neutral; the corresponding 
    # values have already been aggregated per word
    def reducer_count_words(self, word, counts):
        yield (None, (sum(counts), word))
    
    def reducer_find_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)  # returns a single tuple with its first element being the 
        # total count/freq of the most popular word, and the second one being the actual utterrance of this word
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_words,
                combiner=self.combiner_count_words,
                reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
            
        ]
            
if __name__ == "__main__":
    MRMostUsedWord.run()