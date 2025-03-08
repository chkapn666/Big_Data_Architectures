from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')
rdd = sc.wholeTextFiles('*.txt')

rdd.foreach(lambda f: print('{0}:\n{1}\n\n'.format(f[0], f[1])))
