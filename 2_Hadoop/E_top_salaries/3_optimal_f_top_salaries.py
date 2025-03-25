#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 20:03:48 2025

@author: chkapsalis

Now our implementation would be even more efficient if we had each mapper generate top 10 salaries & gross pays
of the data it processes 
and then pass those to the reducer
~ less data is transmitted through the network (so we incur lower overhead due to network communication delays)
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')
class salarymax(MRJob):
    linec = 0
    
    ## "Mapper Initializer"
    # Heap initialization - a heap can be used to find the min or max of a list. Python's heapq module 
    # only does minimization.
    # It is only constructed as a plain python list - we 'heapify' these lists (and allow for the efficient heap implementation) 
    # by implementing heapq methods on it. The documentation of heapq is very short and you can find it here: https://docs.python.org/3/library/heapq.html
    def init_mapper(self):
        self.salaries = []  
        self.gross = []

    ## "Core Mapper"
    # This is the mapping fnc to implement as long as we have data to work on
    def mapper(self, _, line):
        # as before, this is the one to read the input text file line by line, ignoring the leading header line
        salarymax.linec += 1
        
        # ignoring the top/header line
        if salarymax.linec == 1:
            return
        
        # normally processing the rest of the lines
        row = dict(zip(cols, line.split(',')))
        if row['Gender'] == 'F':
            
            # we push all new key-val pairs we come across (aka all the lines of the input file, aka all the available data)
            # The following line turns our 'self.salaries' list of tuples into a heap object. Heap objects implement an extremely efficient
            # algo for creating a tree-like structure that sorts elements in ascending order - here we add numeric values as the first element of tuples, so they will
            # be sorted on those 
            heapq.heappush(self.salaries, (float(row['AnnualSalary']), row['Name']))  
            # making sure we only keep the 10 x (key-val) pairs; using 'pop', we get rid of the MINIMUM salary object each time, so at the end 
            # there will remain those tuples that have the max 'AnnualSalary' values
            if len(self.salaries) > 10:
                heapq.heappop(self.salaries)  # A heap looks like a tree, with the root node having the min value. WARNING: if the elements of a heap are iterators, 
                # the heap only compares them via considering their FIRST non-iterator element's value - so we add the sum we want to compare as the first
                # element of the tuples we push into the heap structure
            
            # same with 'GrossPay' - turn it into a heap and start getting rid of the minimum values
            heapq.heappush(self.gross, (float(row['GrossPay']), row['Name']))
            if len(self.gross) > 10:
                heapq.heappop(self.gross)


    ## "Mapper Finalizer"
    # But how does the mapper know that there are more data to work with? - the runtime (aka hadoop's execution framework)
    # takes care of this; we just need to state that the core job ought to be performed on all availalble data - all the lines in an available line
    # 
    def final_mapper(self):
        while self.salaries:
            yield 'salary', heapq.heappop(self.salaries)
        while self.gross:
            yield 'gross', heapq.heappop(self.gross)

    # Intermediate results from its mapping finalizer are shuffled 
    
    # So this whole implementation is much faster than the previous because we transfer part of the whole processing to the mapper, which is implemented
    # on the node where the data are initially stored - this way, we prevent a big proportion of the network overhead we would incur if we were to transfer all
    # data points to the node where the reducer would take place. 


    # Then, they are fed into the reducer for final result production
    def reducer(self, key, values):
        topten = list(values)
        topten.sort(reverse=True)
        for p in topten[:10]:
            yield key, p


    # Defining the workflow of our MapReduce jobs to outline the whole operation we want the computer to execute
    def steps(self):
        return [
            MRStep(
                mapper_init=self.init_mapper,
                mapper=self.mapper,
                mapper_final=self.final_mapper,
                reducer=self.reducer
            ),
        ]


if __name__ == "__main__":
    salarymax.run()