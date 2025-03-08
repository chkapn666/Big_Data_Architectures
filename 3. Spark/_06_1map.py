from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize(range(10))
rdd1 = rdd.map(lambda x: (x, x % 2 == 0))
print(rdd1.collect())
