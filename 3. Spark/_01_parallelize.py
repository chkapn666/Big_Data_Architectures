from pyspark import SparkContext

maxNum = 10000

sc = SparkContext("local[1]", '6107app')
rdd = sc.parallelize(range(maxNum), 5)
print("Number of Partitions: {0}".format(rdd.getNumPartitions()))
print("Action: First element: {0}".format(rdd.first()))
print("Action: sum, mean of values: {0}".format(rdd.sum(), rdd.mean()))

print("Action: RDD first ten values: ")
for n in rdd.take(10):
    print(n)

print("Action: RDD first ten values: ")
rdd.filter(lambda x: x < 10).foreach(print)

rddCollect = rdd.collect()
print("Action: RDD converted to Array[Int]], first ten values: ")
for n in rddCollect[:10]:
    print(n)