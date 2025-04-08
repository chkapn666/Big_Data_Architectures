from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class ClosestToZero(MRJob):

    def mapper_init(self):
        self.closest_heap = []

    def mapper(self, _, line):
        # Each mapper is assigned a disjoint portion of the file (expressed as some number of lines). 
        # Each of them will parse the lines it has been delegated, line-by-line. 
        # I make sure that each line has the appropriate form by imposing exception handling.
        try:
            # Since the numbers appear to have been generated via some pseudo-random number generator, there could 
            # be duplicate values in a given line, slowing down our program due to superfluous processing. 
            row = list(set(map(int, line.split())))  # We ignore the duplicate values on a given line and retain only 1 instance of each
            # Store (abs(value), value) to keep track of sign.
            
            # I retain a heap with (absolute_value, initial_value) elements. The absolute values closest to 
            # zero are actually those that are smallest. So I can implement my solution using min-  heaps. 
            for val in row:
                heapq.heappush(self.closest_heap, (-abs(val), val))

                if len(self.closest_heap) > 10:
                    # Remove the farthest number from zero (based on absolute value).
                    heapq.heappop(self.closest_heap)

        except ValueError:
            return  # Ignore invalid lines.

    def mapper_final(self):
        # Emit top-10 closest integers (aka the 10 lowest absolute values) locally from this mapper.
        for pair in self.closest_heap:
            yield "closest", pair

    def reducer(self, key, values):
        # I do the same as I did for each mapper, but now for the reduced results. 
        # So now i do not need to specify that the first element of each tuple to be considered in a heap 
        # needs to be -abs(its_value); it has already been implemented by the mappers.
        combined_heap = []

        for val in values:
            heapq.heappush(combined_heap, val)

            if len(combined_heap) > 10:
                heapq.heappop(combined_heap)

        # Extract only the original values (not their absolute values).
        yield "10 Numbers Closest to 0", [pair[1] for pair in sorted(combined_heap, key=lambda x: abs(x[1]))]

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
