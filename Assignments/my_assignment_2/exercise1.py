from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class MinMax10(MRJob):

    def mapper(self, _, line):
        try:
            row = list(set(map(int, line.split())))
        except:
            return  # skip bad lines

        row = sorted(row)
        
        # Emit actual 10 smallest and 10 largest from this row
        yield "min", row[:10]
        yield "max", row[-10:]

    # Reducer receives:
    # "min" -> list of small-value sublists
    # "max" -> list of high-value sublists
    def reducer(self, key, values):
        combined = []
        for sublist in values:
            combined.extend(sublist)

        if key == "min":
            yield "10 Smallest Integers", heapq.nsmallest(10, combined)
        elif key == "max":
            yield "10 Largest Integers", heapq.nlargest(10, combined)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == "__main__":
    MinMax10.run()
