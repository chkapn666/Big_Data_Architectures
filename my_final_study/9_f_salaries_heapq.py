from mrjob.job import MRJob
from mrjob.step import MRStep 
import time 
import heapq 

cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')

class SalaryMinMax(MRJob):
    linec = 0 

    def init_mapper(self):
        self.salaries = []
        self.gross = []

    def mapper(self, _, line):
        SalaryMinMax.linec += 1 

        if SalaryMinMax.linec == 1:
            return 

        row = dict(zip(cols, line.lstrip().split(',')))  # so now i have a dict object with keys 'Name', 'Gender', 'AnnualSalary', 'GrossPay'
        if row['Gender'] == 'F':
            heapq.heappush(self.salaries, (float(row['AnnualSalary']), row['Name']))
            if len(self.salaries) > 10:
                heapq.heappop(self.salaries)

            heapq.heappush(self.gross, (float(row['GrossPay']), row['Name']))
            if len(self.gross) > 10:
                heapq.heappop(self.gross)

    def final_mapper(self):
        # each mapper will now have a list with the 10 highest salaries and gross amounts
        # If each yielded a list, then the reducers would have to work their ways through lists of sublists, making it more unintuitive
        # So we want them to emit single values per key-value pair, not grouped lists
        while self.salaries:
            yield "salary", heapq.heappop(self.salaries)  

        while self.gross:
            yield "gross", heapq.heappop(self.gross)      

    def reducer(self, key, values):
        # Now the reducers get lists of elements that belong to the top 10 salaries or gross amounts found by each mapper.
        # We simply sort through them and work our way to the top 10 among them
        values = list(values)
        values.sort(reverse=True)  # by default, sorting takes place based on the first element of iterables
        for top_10_pair in values[:10]:  # keeping only te 
            yield key, top_10_pair[0]

    def steps(self):
        return [
            MRStep(
                mapper_init=self.init_mapper,
                mapper=self.mapper,
                mapper_final=self.final_mapper,
                reducer=self.reducer
                )
        ]

if __name__ == "__main__":
    SalaryMinMax.run()