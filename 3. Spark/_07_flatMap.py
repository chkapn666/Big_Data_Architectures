from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
rdd1 = rdd.map(lambda x: x[-1::-1])
print(rdd1.collect())
rdd2 = rdd.flatMap(lambda x: x[-1::-1])
print(rdd2.collect())
