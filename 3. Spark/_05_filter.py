from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize(range(20))
rdd1 = rdd.filter(lambda x: x % 2 == 0)
print(rdd1.collect())
