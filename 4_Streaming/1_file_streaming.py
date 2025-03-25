from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

sc = SparkContext("local[2]", "WordCount")

### Creating a stream context with interval 10 seconds
ssc = StreamingContext(sc, 10)

### Getting the lines for each interval, reading from a designated folder containing text files
lines = ssc.textFileStream('/streamdata')  # this is an RDD containing line-object RDDs

### Split all lines into a list of words
words = lines.flatMap(lambda line: line.split(" "))

### Calculate word occurrences in each interval
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

### Print calculated counts
wordCounts.pprint()

### In order for the previous processing to occur, we need to execute the following two methods
##DEBUGGING:
print("✅ StreamingContext started. Waiting for files...")
##
ssc.start()  # start the computation
ssc.awaitTermination()  # wait for the computation to terminate