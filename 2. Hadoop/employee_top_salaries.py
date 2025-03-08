from mrjob.job import MRJob

cols = 'Name,AnnualSalary,GrossPay'.split(',')

class salarymax(MRJob):
    linec = 0
    def mapper(self, _, line): # Convert each line into a dictionary
        salarymax.linec += 1
        if salarymax.linec == 1:
            return
        row = dict(zip(cols, line.split(',')))

        yield 'salary', (float(row['AnnualSalary']), row['Name'])
        yield 'gross', (float(row['GrossPay']), row['Name'])

    def reducer(self, key, values):
        topten = list(values)
        topten.sort(reverse=True)
        for p in topten[:10]:
            yield key, p


if __name__ == '__main__':
    salarymax.run()
