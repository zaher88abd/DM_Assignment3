from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# from pyspark.sql.functions import desc

sc = SparkContext("local[2]", "Tweet Streaming App")


ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)
ssc.checkpoint( "file:/home/ubuntu/tweets/checkpoint/")

socket_stream = ssc.socketTextStream("<your_ip>", 5555) # Internal ip of  the tweepy streamer

lines = socket_stream.window(20)

lines.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
# If we want to filter hashtags only
# .filter( lambda word: word.lower().startswith("#") )
words = lines.flatMap( lambda twit: twit.split(" ") )
pairs = words.map( lambda word: ( word.lower(), 1 ) )
wordCounts = pairs.reduceByKey( lambda a, b: a + b ) #.transform(lambda rdd:rdd.sortBy(lambda x:-x[1]))
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
