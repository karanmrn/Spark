from pyspark import SparkContext

def blank_line(line):
    if (len(line)==0):
        myacc.add(1)

sc=SparkContext(appName="KeyWordAmount",master="local[*]")

myrdd = sc.textFile("/Users/karanmanoharan/Downloads/samplefile.txt")

myacc = sc.accumulator(0)

myrdd.foreach(blank_line)

print(myacc.value)