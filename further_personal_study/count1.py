from mrjob.job import MRJob
from mrjob.step import MRStep 
import time 
import re

WORD_RE = re.compile(r"[\w']+")

class MostUsedWord(MRJob):
    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far ""
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
            combiner=self.combiner_count_words,
            reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

if __name__ == "__main__":
    MostUsedWord.run()


# IF I only wanted to run the mapper and combiner to see their result: 
#python count1.py --mapper --step-num=0 data.txt > mapper_output.txt
#sort mapper_output.txt > sorted_mapper_output.txt
#python count1.py --combiner --step-num=0 < sorted_mapper_output.txt
