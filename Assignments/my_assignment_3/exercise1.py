from pyspark import SparkContext
import os
os.chdir('/Users/chkapsalis/Documents/GitHub/Big_Data_Architectures/Assignments/my_assignment_3')

# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

spark = SparkContext("local[1]", "app")


# This exercise is asking for a vanilla spark application - no usage of Spark DFs will be made.
# As such, the ingestion of the selected csv file will be different; i need to manually remove its header column!
# Then i will be ready to ingest it:
cols = 'As Of,NAV per Share,Daily NAV Change,Daily NAV Change %'.split(',')

rdd = spark.textFile('file:///' + os.getcwd() + '/BGF-World-Technology-Fund-Class-A2-EUR_fund_noH.csv')


import datetime 

def parse(line):
    fields = line.split(',')
    try:
        date = datetime.datetime.strptime(fields[0], "%d/%m/%Y")  # Correct format
        nav = float(fields[1])
        daily = float(fields[2])
        pct = float(fields[3])
        return (nav, daily, pct)
    except:
        return None


parsed_rdd = rdd.map(parse).filter(lambda x: x is not None)


# So i can iterate over them and calculate the max & min daily and pct changes
# At the same time, I can accumulate the total count of values, their total sum, and the min and max vals
# Then i am able to calculate the avg
# Then i will iterate over it again to calculate the std

# So i will first utilize the 'aggregate' transformation
# Aggregate: zerovalue like (count, sum of NAV, min/max of daily change, min/max of pct change)
zero = (0, 0.0, float('inf'), float('-inf'), float('inf'), float('-inf'))  # count, sum_nav, min_daily, max_daily, min_pct, max_pct

# Within-partition processing (happening directly on the nodes where data is stored)
# Each worker will be appointed (in the background) various lines of this file; 
# I want each of them to count the number of NAV values they come across and their sum,
# plus the min daily/pct and max daily/pct values they come across.


def seqOp(acc, v):
    count, total_nav, min_daily, max_daily, min_pct, max_pct = acc
    nav, daily, pct = v
    return (
        count + 1,
        total_nav + nav,
        min(min_daily, daily),
        max(max_daily, daily),
        min(min_pct, pct),
        max(max_pct, pct)  # fixed typo
    )

# Combination of intermediate results to produce final results:
# I need the sum of NAV counts, the sum of NAV sums, the min out of all daily/pct mins, and the max out of all daily/pct maxs
def combOp(acc1, acc2):
    return (
        acc1[0] + acc2[0],
        acc1[1] + acc2[1],
        min(acc1[2], acc2[2]),
        max(acc1[3], acc2[3]),
        min(acc1[4], acc2[4]),
        max(acc1[5], acc2[5])
    )



count_nav, sum_nav, min_daily, max_daily, min_pct, max_pct = parsed_rdd.aggregate(zero, seqOp, combOp)
avg_nav = sum_nav / count_nav

print(f"Total NAV Entries: {count_nav}")
print(f"Average NAV: {avg_nav:.4f}")
print(f"Min Daily Change: {min_daily}, Max Daily Change: {max_daily}")
print(f"Min % Change: {min_pct}, Max % Change: {max_pct}")
