from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize([1, 3, 6, 8, 3, 4, 6, 7])
sum = rdd.fold(0, lambda x, y: x + y)
minv = rdd.fold(rdd.take(1)[0], lambda x, y: min(x, y))
maxv = rdd.fold(rdd.take(1)[0], lambda x, y: max(x, y))
print('Min: {0}, max: {1}, sum: {2}'.format(minv, maxv, sum))
