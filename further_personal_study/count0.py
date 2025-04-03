from mrjob.job import MRJob
from mrjob.step import MRStep 
import time 

class MRWordCount(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield word, 1 
            # each mapper producers 

    def reducer(self, word, counts): 
        yield word, sum(counts)


if __name__ == "__main__":
    MRWordCount.run()

# If i want to write the whole thing and take a look at the final results: ```python count0.py data.txt```
# If i want to inspect the results of the mappers (aka what is going into the reducers): ```python count0.py --mapper --step-num=0 data.txt```
# To run on a data file on HDFS: ```python count0.py r- hadoop hdfs:///data.txt```
