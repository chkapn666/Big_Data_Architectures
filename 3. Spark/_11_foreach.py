from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')
rdd = sc.parallelize(range(5))

rdd.foreach(lambda x: print('Element {0}'.format(x)))

