from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

sc = SparkContext("local[2]", "WordCount")

### Creating a stream context with interval 10 seconds
ssc = StreamingContext(sc, 10)

# A server is just a script listening to a particular port, indefinitely running on localhost
# After running this script, i can send data do it from my client (aka local pc) through a 
# command like "cat hamlet.txt | nc -lk localhost 9999"
lines = ssc.socketTextStream('localhost', 9999)  # this is an RDD containing line-object RDDs


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