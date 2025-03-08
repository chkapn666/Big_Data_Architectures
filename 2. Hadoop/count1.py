from mrjob.job import MRJob
import datetime

class MRWordCount(MRJob):
    def mapper(self, key, line):
        with open('/tmp/Mapper.log', 'a+') as logf:
            logf.write('Mapper [{0}]: ({1}, {2})\n' \
                       .format(datetime.datetime.now(), key, line.encode()))
        for word in line.split():
            yield word, 1

    def reducer(self, word, counts):
        countsList = list(counts)
        with open('/tmp/Reducer.log', 'a+') as logf:
            logf.write('Reducer [{0}]: ({1}, {2})\n'
                       .format(datetime.datetime.now(), word, countsList))
        yield word, sum(countsList)


if __name__ == '__main__':
    MRWordCount.run()
