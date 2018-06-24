from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression, NaiveBayes, LinearSVC

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import Row, SparkSession

# from pyspark.sql.functions import desc
# print("Path",sys.argv[1].__str__())

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def printer(x):
    return "Tweet text: " + x


sc = SparkContext("local[2]", "Tweet Streaming App")


ssc = StreamingContext(sc, 10)
# sqlContext = SQLContext(sc)
ssc.checkpoint("./tweets/checkpoint/")

# Internal ip of  the tweepy streamer
socket_stream = ssc.socketTextStream("127.0.0.1", 9200)
print("load model")
lp = PipelineModel.load("piplineModel/")
print("model  ", type(lp))


lines = socket_stream.window(20)
print("Type of data", type(lines))

# predictions = lp.transform(lines)
# lines.map(lambda x: 'Tweets in this batch: %s' % x).pprint()
# predictions['prediction'].map(lambda x: printer(x)).pprint()

# Convert RDDs of the words DStream to DataFrame and run SQL query


def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame.
        wordsDataFrame.createOrReplaceTempView("words")
        print("Data...............")
        print("Type of data 1 ", type(wordsDataFrame))
        print("Type of data list ", wordsDataFrame.columns)
        wordsDF = wordsDataFrame.withColumnRenamed('word', 'SentimentText')
        print("Type of data list ", wordsDF.columns)
        print("Type of data 1 ", type(wordsDF))
        # print("model  ", lp)
        # print("model  ", type(lp))

        # predictions = lp.transform(wordsDF)
        # predictions.filter(predictions['prediction'] == 1) \
        # .select("SentimentText", "Sentiment", "probability", "label", "prediction") \
        # .orderBy("probability", ascending=False) \
        # .show(n=10, truncate=30)

        # print("Type of data 2 ",type(predictions))
        # # Do word count on table using SQL and print it
        # wordCountsDataFrame = \
        #     spark.sql("select word, count(*) as total from words group by word")
        # wordCountsDataFrame.show()
    except:
        pass


lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
