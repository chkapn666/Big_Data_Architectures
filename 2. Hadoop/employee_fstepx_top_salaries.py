from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')

class salarymax(MRJob):
    linec = 0
    def init_mapper(self):
        self.salaries = []
        self.gross = []

    def mapper(self, _, line):
        salarymax.linec += 1
        if salarymax.linec == 1:
            return
        row = dict(zip(cols, line.split(',')))
        if row['Gender'] == 'F':
            heapq.heappush(self.salaries, (float(row['AnnualSalary']), row['Name']))
            if len(self.salaries) > 10:
                heapq.heappop(self.salaries)
            heapq.heappush(self.gross, (float(row['GrossPay']), row['Name']))
            if len(self.gross) > 10:
                heapq.heappop(self.gross)

    def final_mapper(self):
        while self.salaries:
            yield 'salary', heapq.heappop(self.salaries)
        while self.gross:
            yield 'gross', heapq.heappop(self.gross)

    def reducer(self, key, values):
        topten = list(values)
        topten.sort(reverse=True)
        for p in topten[:10]:
            yield key, p

    def steps(self):
        return [
            MRStep(
                mapper_init=self.init_mapper,
                mapper=self.mapper,
                mapper_final=self.final_mapper,
                reducer=self.reducer),
        ]

if __name__ == '__main__':
    salarymax.run()
