from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile('Hamlet.txt')
counts = lines.flatMap(lambda line: line.split(' ')) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda x, y: x + y) \
              .collect()

print(counts)