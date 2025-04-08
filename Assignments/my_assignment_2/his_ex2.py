from mrjob.job import MRJob
import heapq

Num = 10


class Closest0(MRJob):

    @staticmethod
    def closest0(s):
        closest0 = []
        for n in s:
            if len(closest0) < Num:
                heapq.heappush(closest0, (-abs(n), n))
            elif abs(n) < abs(closest0[0][1]):
                heapq.heappop(closest0)
                heapq.heappush(closest0, (-abs(n), n))
        return [x[1] for x in closest0]

    def mapper(self, _, line):
        yield 'Closest 0', Closest0.closest0(set(map(int, line.split())))


    def reducer(self, key, values):
        ints = []
        for intl in values:
            ints += intl
        yield 'Closest 0', Closest0.closest0(set(ints))


if __name__ == '__main__':
    Closest0.run()
