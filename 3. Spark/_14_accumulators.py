from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

acc = sc.accumulator(0)

rdd = sc.parallelize([1, 3, 5, 7, 9])
rdd.foreach(lambda x: acc.add(x))
print('Sum of odds: {0}'.format(acc))

from pyspark.accumulators import AccumulatorParam
class strAcc(AccumulatorParam):
    def zero(self, _):
        return ''
    def addInPlace(self, var, val):
        return var + val

accs = sc.accumulator('', strAcc())
rdd1 = sc.parallelize(['a', 'b', 'c', 'd', 'e'])
rdd1.foreach((lambda x: accs.add(x)))
print('Letter concats: {0}'.format(accs))