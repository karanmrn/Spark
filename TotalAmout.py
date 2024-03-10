from pyspark import SparkContext

def load_boringwords():
    boring_words=set(line.strip() for line in open("/Users/karanmanoharan/Downloads/boringwords.txt"))
    return boring_words



sc=SparkContext(appName="KeyWordAmount",master="local[*]")

name_set= sc.broadcast(load_boringwords())

initial_rdd=sc.textFile("/Users/karanmanoharan/Downloads/bigdatacampaigndata-201014-183159.csv")

rdd1=initial_rdd.map(lambda x: (float(x.split(",")[10]),x.split(",")[0]))

rdd2=rdd1.flatMapValues(lambda x:x.split(" "))

rdd3=rdd2.map(lambda x: (x[1].lower(),x[0]))

filter_rdd=rdd3.filter(lambda x: (x[0] not in name_set.value))

rdd4=filter_rdd.reduceByKey(lambda x,y:x+y)

total=rdd4.sortBy(lambda x:x[1],ascending=False)

result =total.take(20)

for x in result:
    print(x)