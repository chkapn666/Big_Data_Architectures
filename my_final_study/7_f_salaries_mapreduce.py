from mrjob.job import MRJob
from mrjob.step import MRStep 
import time 
import heapq 

cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')

class SalaryMinMax(MRJob):
    linec = 0 


    def mapper(self, _, line):
        SalaryMinMax.linec += 1 

        if SalaryMinMax.linec == 1:
            return 

        row = dict(zip(cols, line.lstrip().split(',')))  # so now i have a dict object with keys 'Name', 'Gender', 'AnnualSalary', 'GrossPay'
        if row['Gender'] == 'F':
            yield "salary", (float(row['AnnualSalary']), row['Name'])  # everything is read as string and we need to parse it properly!!!
            yield "gross", (float(row['GrossPay']), row['Name'])
        

    def reducer(self, key, values):
        values = list(values)
        values.sort(reverse=True)  # by default, sorting takes place based on the first element of iterables
        for top_10_pair in values[:10]:
            yield key, top_10_pair[0]

if __name__ == "__main__":
    SalaryMinMax.run()