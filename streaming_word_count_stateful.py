from pyspark.shell import sc
from pyspark.streaming import *

sc.setLogLevel("ERROR")

# creating spark streaming context
ssc = StreamingContext(sc, 2)

# line in dstream
lines = ssc.socketTextStream("localhost", 9998)

ssc.checkpoint(".")


def updatefunc(newValues, previousState):
    if previousState is None:
        previousState = 0
    return sum(newValues, previousState)


# words in transformed dstream
words = lines.flatMap(lambda x: x.split())
pairs = words.map(lambda x: (x, 1))
word_counts = pairs.reduceByKey(updatefunc)
word_counts.pprint()
ssc.start()
ssc.awaitTermination()
