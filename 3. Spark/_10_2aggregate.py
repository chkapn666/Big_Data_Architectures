from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize([("A", 10), ("B", 20), ("B", 30), ("C", 40), ("D", 30), ("E", 60)])
r = rdd.aggregate(0, lambda acc, v: acc + v[1], lambda acc1, acc2: acc1 + acc2)
print('Output: {0}'.format(r))
