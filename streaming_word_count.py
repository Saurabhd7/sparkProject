from pyspark.shell import sc
from pyspark.streaming import *

sc.setLogLevel("ERROR")

# creating spark streaming context
ssc = StreamingContext(sc, 2)

# line in dstream
lines = ssc.socketTextStream("localhost", 9998)

# words in transformed dstream
words = lines.flatMap(lambda x: x.split())
pairs = words.map(lambda x: (x, 1))
word_counts = pairs.reduceByKey(lambda x, y: x + y)
word_counts.pprint()
ssc.start()
ssc.awaitTermination()
