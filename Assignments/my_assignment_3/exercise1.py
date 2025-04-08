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

rdd = spark.textFile('file:///' + os.getcwd() + '/BGF-WTF-A2-EUR_fund1.csv')#_noH.csv')


import datetime 

def parse(line):
    """Function that takes care of parsing and importing data only from valid lines of the input file."""
    fields = line.split(',')
    try:
        date = datetime.datetime.strptime(fields[0], "%d-%b-%y") 
        nav = float(fields[1])
        daily = float(fields[2])
        pct = float(fields[3])
        return (nav, daily, pct, date)
    except:
        return None

print("LINES OF THE INITIAL RDD", rdd.count())


parsed_rdd = rdd.map(parse).filter(lambda x: x is not None).cache()  # this rdd will be used multiple times in my code,
    # so I cache it in primary memory to avoid spark from performing the transformations that led to it again and again
print("LINES OF THE PARSED RDD", parsed_rdd.count())  # it will be rdd.count() - 1, because it ignores the header line




### Answering questions 1 and 2 ###
parsed_rdd_q1_q2 = parsed_rdd.map(lambda x: (x[0], x[1], x[2]))

# So i can iterate over them and calculate the max & min daily and pct changes
# At the same time, I can accumulate the total count of values, their total sum, and the min and max vals
# Then i am able to calculate the avg
# Then i will iterate over it again to calculate the std

# So i will first utilize the 'aggregate' transformation
# Aggregate: zerovalue like (count, sum of NAV, min/max of daily change, min/max of pct change)
zero = (0, 0.0, float('inf'), float('-inf'), float('inf'), float('-inf'))  # count, sum_nav, min_daily, max_daily, min_pct, max_pct

# Within-partition processing (happening directly on the nodes where data is stored):
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
        max(max_pct, pct) 
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



count_nav, sum_nav, min_daily, max_daily, min_pct, max_pct = parsed_rdd_q1_q2.aggregate(zero, seqOp, combOp)
avg_nav = sum_nav / count_nav

print(f"Total NAV Entries for the full history: {count_nav}")
print(f"Average NAV for the full history: {avg_nav:.4f}")
print(f"Min Daily Change for the full history: {min_daily}, Max Daily Change: {max_daily}")
print(f"Min % Change for the full history: {min_pct}, Max % Change: {max_pct}")

std_nav = parsed_rdd_q1_q2 \
            .map(lambda x: (x[0] - avg_nav) ** 2) \
            .mean()
std_nav = std_nav ** 0.5
print(f"Std NAV for the full history:", round(std_nav, 2), "monetary units.")



### Answering question 3 ### 
parsed_rdd_q3 = parsed_rdd.filter(lambda x: x[-1].year == 2020)
count_nav, sum_nav = parsed_rdd_q3.map(lambda x: x[0]) \
                .aggregate(
                    (0,0),
                    (lambda acc, v: (acc[0]+1, acc[1]+v)),
                    (lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
                )           
print('Avg nav for 2020:', round(sum_nav/count_nav, 2))


### Answering question 4 ### 
# I create (month,single_nav) key-value pairs per sub-rdd
# I reduce by key to find the total count and total sum for each key
# I mapValues to make sure that my final calculation is performed PER KEY (cannot control this when using '.aggregate'!!!)
rs = parsed_rdd \
                .map(lambda x: ((x[-1].year,x[-1].month), (1, x[0]))) \
                .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])) \
                .mapValues(lambda x: round(x[1] / x[0],2)) \
                .sortBy(lambda x: (x[0][0], x[0][1]), ascending=False) \
                .collect()
print(rs)