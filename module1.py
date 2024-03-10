from pyspark import SparkContext
import sys





sc = SparkContext("local[*]", "CustomerTotal")
rdd1 = sc.textFile("/Users/karanmanoharan/Documents/customerorders-201008-180523.txt")
rdd2 = rdd1.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))
rdd3 = rdd2.reduceByKey(lambda x,y: x+y)
rdd4 = rdd3.sortBy(lambda x: x[1],False)
result = rdd4.collect()
for a in result:
    print(a)
