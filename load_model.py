from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple
# from pyspark.sql.functions import desc

sc = SparkContext("local[2]", "Streaming App")


ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)
#ssc.checkpoint( "file:/home/ubuntu/tweets/checkpoint/")

# Internal ip of  the tweepy streamer
socket_stream = ssc.socketTextStream("127.0.0.1", 9200)
lpModel = PipelineModel.load("piplineModel/")

lines = socket_stream.window(20)
lines.pprint()
fields = ("SentimentText")
Tweet = namedtuple('Tweet', fields)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def do_something(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Tweet] to DataFrame
        rowRdd = rdd.map(lambda w: Tweet(w))
        linesDataFrame = spark.createDataFrame(rowRdd)

        linesDataFrame = lpModel.transform(linesDataFrame)

        # Creates a temporary view using the DataFrame
        linesDataFrame.createOrReplaceTempView("tweets")

        # Do tweet character count on table using SQL and print it
        lineCountsDataFrame = spark.sql(
            "select SentimentText, prediction  from tweets")
        print("Show data")
        lineCountsDataFrame.show()
        lineCountsDataFrame.write.format(
            "com.databricks.spark.csv").save("dirwithcsv")
    except:
        pass


# key part!
lines.foreachRDD(do_something)

ssc.start()
ssc.awaitTermination()
