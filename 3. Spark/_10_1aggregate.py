from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize([1, 2, 4, 5, 2, 4, 6, 1])

r = rdd.aggregate(0, (lambda acc, v: acc + v), (lambda acc1, acc2: acc1 + acc2))
print("Output: {0}".format(r))