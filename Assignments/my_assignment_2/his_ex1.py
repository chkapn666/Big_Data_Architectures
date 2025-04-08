from mrjob.job import MRJob


class high_low_10(MRJob):
    def mapper(self, _, line):
        ints = list(set(map(int, line.split())))
        ints.sort(reverse=True)
        yield 'HighLow10', (ints[:10], ints[-10::])

    def reducer(self, key, values):
        ints_high = []
        ints_low = []
        for ilist in values:
            ints_high += ilist[0]
            ints_low += ilist[1]
        ints_high = list(set(ints_high))
        ints_high.sort(reverse=True)
        ints_low = list(set(ints_low))
        ints_low.sort(reverse=True)
        yield 'High 10', ints_high[:10]
        yield 'Low 10', ints_low[-10::]


if __name__ == '__main__':
    high_low_10.run()
