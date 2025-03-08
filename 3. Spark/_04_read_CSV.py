from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')
rdd = sc.textFile('*.csv')

rdd1 = rdd.map(lambda x: x.split(','))
for r in rdd1.collect():
    print(r)
