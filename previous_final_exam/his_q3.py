from mrjob.job import MRJob
from mrjob.step import MRStep
from functools import reduce
import heapq


class BTC(MRJob):

    top10closing = []

    def init_mapper(self):
        self.top10closing = []

    def mapper(self, _, line):
        if line == 'ticker,date,open,high,low,close':
            return
        row = line.split(',')
        date = row[1]
        year, month, day = map(int, row[1].split('-'))
        high, low, close = map(float, row[3:])

        # 1. The average closing price of BTC in 2020.
        if year == 2020:
            yield 'Average closing price in 2020', close

        # 2. The number of dates for which the closing price was over 60000 USD.
        if close > 60000:
            yield 'Number of days with closing price > 60000', 1

        # 3. The number of days for which the closing price was less than the average of the highest and lowest price.
        if close < (high + low) / 2:
            yield 'Number of days with close < (low + high) / 2', 1

        # 4. The ten highest closing prices and the corresponding dates.
        heapq.heappush(self.top10closing, (close, date))
        if len(self.top10closing) > 10:
            heapq.heappop(self.top10closing)

        # 5. The year and month for which the average closing price was between 20000 and 22000 USD.
        yield ('Year-month with average closing between 20K and 22K', year, month), close

    def final_mapper(self):
        while self.top10closing:
            yield 'Ten highest closing prices', heapq.heappop(self.top10closing)

    def reducer(self, key, values):
        if key == 'Average closing price in 2020':
            s, c = reduce(lambda x, y: (x[0] + y, x[1] + 1), values, (0, 0))
            yield key, round(s / c, 2)

        if key == 'Number of days with closing price > 60000':
            yield key, sum(1 for _ in values)

        if key == 'Number of days with close < (low + high) / 2':
            yield key, sum(1 for _ in values)

        if key == 'Ten highest closing prices':
            topten = list(values)
            topten.sort(reverse=True)
            for p in topten[:10]:
                yield key, f'Price: {p[0]}, date: {p[1]}'

        if key[0] == 'Year-month with average closing between 20K and 22K':
            s, c = reduce(lambda x, y: (x[0] + y, x[1] + 1), values, (0, 0))
            if 20000 <= s / c <= 22000:
                yield key[0], f'Year: {key[1]}, Month: {key[2]}, Average closing: {round(s / c, 2)}'

    def steps(self):
        return [
            MRStep(
                mapper_init=self.init_mapper,
                mapper=self.mapper,
                mapper_final=self.final_mapper,
                reducer=self.reducer),
        ]


if __name__ == '__main__':
    BTC.run()
