from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')
rdd = sc.textFile('*.txt', 3)

print('Number of partitions: {0}'.format(rdd.getNumPartitions()))

rdd.foreach(lambda f: print(f))
