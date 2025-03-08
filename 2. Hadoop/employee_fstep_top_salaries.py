from mrjob.job import MRJob
from mrjob.step import MRStep

cols = 'Name,Gender,AnnualSalary,GrossPay'.split(',')

class salarymax(MRJob):
    linec = 0
    def filter(self, _, line):
        salarymax.linec += 1
        if salarymax.linec == 1:
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

if __name__ == '__main__':
    salarymax.run()
