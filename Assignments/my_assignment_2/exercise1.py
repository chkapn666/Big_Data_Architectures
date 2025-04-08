from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class MinMax10(MRJob):

    def mapper(self, _, line):
        # The file we input contains multiple numbers per row, space-delimited.
        # We need to make sure that every row we parse is valid. So we implement exception handling.
        # Mappers are assigned particular lines of a given file; each reads their part line-by-line.
        try:
            row = list(set(map(int, line.split())))
        except:
            return  # ignore invalid lines

        row = sorted(row)
        
        # Emit actual 10 smallest and 10 largest from this row
        yield "min", row[:10]
        yield "max", row[-10:]

    # Reducer receives:
    # "min" -> list of small-value sublists, with each sublist being output by a different mapper that 
    # has processed the lines delegated to it.
    # "max" -> list of high-value sublists
    def reducer(self, key, values):
        combined = []
        for sublist in values:
            combined.extend(sublist)

        combined = list(set(combined))  # getting rid of duplications

        if key == "min":
            yield "10 Smallest Integers", heapq.nsmallest(10, combined)
        elif key == "max":
            yield "10 Largest Integers", heapq.nlargest(10, combined)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == "__main__":
    MinMax10.run()
