from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class ClosestToZero(MRJob):

    def mapper_init(self):
        self.closest_heap = []

    def mapper(self, _, line):
        try:
            row = list(set(map(int, line.split())))
            # Store (abs(value), value) to keep track of sign.
            
            for val in row:
                heapq.heappush(self.closest_heap, (abs(val), val))

            if len(self.closest_heap) > 10:
                # Remove the farthest number from zero (based on absolute value).
                heapq.heappop(self.closest_heap)

        except ValueError:
            return  # Skip invalid lines.

    def mapper_final(self):
        # Emit top-10 closest integers locally from this mapper.
        for pair in self.closest_heap:
            yield "closest", pair

    def reducer(self, key, values):
        # Combine all tuples and find the global top-10 closest to zero.
        combined_heap = list(values)
        result = heapq.nsmallest(10, combined_heap)
        # Extract only the original values (not their absolute values).
        yield "10 Numbers Closest to 0", [pair[1] for pair in result]

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                mapper_final=self.mapper_final,
                reducer=self.reducer
            )
        ]

if __name__ == "__main__":
    ClosestToZero.run()
