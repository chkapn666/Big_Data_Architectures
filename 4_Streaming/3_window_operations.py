from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Ensure Java is properly configured (Mac-specific workaround)
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

# Create Spark context and streaming context with a 10-second batch interval
sc = SparkContext("local[2]", "WindowedWordCount")
ssc = StreamingContext(sc, 10)

# Set checkpoint directory (required for window operations)
ssc.checkpoint("checkpoint-directory")  # You can change this to an absolute path if needed

# Connect to a socket stream on localhost:9999
lines = ssc.socketTextStream('localhost', 9999)

# Transformations
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))

# Apply window operation: 60-second window, sliding every 10 seconds
windowedWordCounts = pairs.reduceByKeyAndWindow(
    lambda x, y: x + y,  # reduce function
    lambda x, y: x - y,  # inverse reduce function for efficiency
    windowDuration=60,   # window size in seconds
    slideDuration=10     # slide interval in seconds
)

# Print the rolling word counts
windowedWordCounts.pprint()

print("✅ StreamingContext started. Listening for data at localhost:9999...")
ssc.start()
ssc.awaitTermination()


# !!!
# Every 10 seconds, Spark will print word counts based on the last 60 seconds of data.
# !!!