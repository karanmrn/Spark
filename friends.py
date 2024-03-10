from pyspark import SparkContext


def parse_line(line):
    fields = line.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return age, friends


sc = SparkContext(appName="Friends", master="local[*]")
lines = sc.textFile("/Users/karanmanoharan/Downloads/friendsdata-201008-180523.txt")
rdd = lines.map(parse_line)

total_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
average = total_age.mapValues((lambda x: x[0]/x[1]))

result = average.collect()

for a in result:
    print(a)
