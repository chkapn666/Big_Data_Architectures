from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class MinMaxInt(MRJob):

    def mapper_init(self):
        self.local_values = []

    def mapper(self, _, line):
        try:
            row_value = int(line.strip())
            self.local_values.append(row_value)
        except ValueError:
            return

    def mapper_final(self):
        # From each mapper, emit:
        # - 10 smallest directly
        # - 10 largest directly
        smallest = heapq.nsmallest(10, self.local_values)
        largest = heapq.nlargest(10, self.local_values)

        while smallest:
            yield "min", heapq.heappop(smallest)

        while largest:
            yield "max", heapq.heappop(largest)
    # so each mapper will produce 10+10 pairs like ("min", numeric_value_of_local_mins_1), ("max", numeric_value_of_local_maxs_2)

    def reducer(self, key, values):
        all_values = list(values)

        if key == "max":
            result = heapq.nlargest(10, all_values)
            yield "10 Largest Integers", sorted(result, reverse=True)

        elif key == "min":
            result = heapq.nsmallest(10, all_values)
            yield "10 Smallest Integers", sorted(result)

    def steps(self):
        return [MRStep(
            mapper_init=self.mapper_init,
            mapper=self.mapper,
            mapper_final=self.mapper_final,
            reducer=self.reducer
        )]

if __name__ == "__main__":
    MinMaxInt.run()
