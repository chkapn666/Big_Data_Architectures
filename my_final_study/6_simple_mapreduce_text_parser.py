from mrjob.job import MRJob
from mrjob.step import MRStep 
import time 

class WordCounter(MRJob):

    def mapper(self, _, line):  # by default, hadoop reads a specified (sys) text file line-by-line
        line = line.lstrip()
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1


    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    WordCounter.run()