from pyspark import SparkContext
# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

sc = SparkContext("local","app")

###########
lines = sc.textFile('file:///' + os.getcwd() + '/data.txt')

counts = lines.flatMap(lambda line: line.split()) \
            .map(lambda word: (word.lower(), 1)) \
            .reduceByKey(lambda x,y: x + y) \
            .sortBy(lambda kv: kv[1], ascending=False) \
            .take(3) 


print(counts)