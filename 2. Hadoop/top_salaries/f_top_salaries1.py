#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 20:03:48 2025

@author: chkapsalis

This makes our script more intuitive; we decouple filtering from mapping, and define explicitly the MapReduce steps to be applied 

"""

from mrjob.job import MRJob 
from mrjob.step import MRStep 


cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')

class SalaryMax(MRJob):
    linec = 0 

    def filter(self, _, line):  # convert each line into a dictionary
        SalaryMax.linec += 1 
        
        # ignore the very first line / "header line" 
        if SalaryMax.linec == 1:
            return 
    
        row = dict(zip(cols, line.split(',')))
        if row['Gender'] == 'F':
            yield 'salary', (float(row['AnnualSalary']), row['Name'])
            yield 'gross', (float(row['GrossPay']), row['Name'])
        
    
    def mapper(self, key, values):
        yield key, values
    
    def reducer(self, key, values):
        topten = list(values)
        topten.sort(reverse=True)
        for p in topten[:10]:
            yield key, p
            
    def steps(self):
        return [
            MRStep(mapper=self.filter),
            MRStep(mapper=self.mapper, 
                       reducer=self.reducer)
        ]
            
if __name__ == "__main__":
    SalaryMax.run()