# here it is more reasonable to use the combiner instead of a heap; 
# Just finding the max between every mapper's designated part of the text document and generalizing based on that could lead to misconceptions; e.g. we could have
# a word that happens to appear a lot of times in a particular portion of the file captured by a single mapper, whereas it is non-existent in other portions of the file 
# So heaps are only used to retain the relative rankings of numeric values referring to different observations (e.g. the salaries of different employees); 
# but when global counting is needed, we need to have a global scope of the 
# actual state before aggregating values. 
# We need to find all "sub-counts" for every word among every portion of the file and sum them all together.
from mrjob.job import MRJob
from mrjob.step import MRStep 
import heapq 

class WordCountRefined(MRJob):

    def mapper_get_words(self, _, line):
        for word in line.lstrip().split():
            yield word.lower(), 1

    def combiner_count_words(self, word, occurrences):
        yield word, sum(occurrences)

    def reducer_count_words(self, word, mapper_count):
        # sum all counts and return (count, word)
        yield None, (sum(mapper_count), word)

    def reducer_max_freq_words(self, _, counts_tuples):
        # Convert generator to list to avoid serialization issues
        counts_list = list(counts_tuples)
        #yield max(counts_list)  # max by default compares by first tuple element
        
        # we could also keep the top 5 words:
        counts_list.sort(reverse=True)
        for pair in counts_list[:5]:
            yield pair

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_words,
                combiner=self.combiner_count_words,  # this groups all key-value pairs on the reducer side prior to implementing the final reducer 
                # expressed aggregation function to produce a final result
                reducer=self.reducer_count_words
            ),  # so now we have a generator full of sublists, each of them containing the all counts for each word in a way that can easily be sorted
            MRStep(
                reducer=self.reducer_max_freq_words
            )
        ]

if __name__ == "__main__":
    WordCountRefined.run()
