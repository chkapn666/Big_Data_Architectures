#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 19:58:42 2025

@author: chkapsalis
"""

from mrjob.job import MRJob 

cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')

class SalaryMax(MRJob):
    linec = 0 
    
    def mapper(self, _, line):  # convert each line into a dictionary
        SalaryMax.linec += 1 
        
        # ignore the very first line / "header line" 
        if SalaryMax.linec == 1:
            return 
    
        row = dict(zip(cols, line.split(',')))
        if row['Gender'] == 'F':
            yield 'salary', (float(row['AnnualSalary']), row['Name'])
            yield 'gross', (float(row['GrossPay']), row['Name'])
        
    
    def reducer(self, key, values):
        topten = list(values)
        topten.sort(reverse=True)
        for p in topten[:10]:
            yield key, p
            
if __name__ == "__main__":
    SalaryMax.run()