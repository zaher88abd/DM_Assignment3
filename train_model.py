from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import col

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression, NaiveBayes, LinearSVC

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

#from pyspark.ml import PipelineModel

sc = SparkContext()
sqlContext = SQLContext(sc)
data = sqlContext.read.format('com.databricks.spark.csv').options(
    header='true', inferschema='true').load('Tweets_new.csv')

drop_list = ['ItemID']
data = data.select(
    [column for column in data.columns if column not in drop_list])
data.show(5)

data.printSchema()

data.groupBy("Sentiment") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

data.groupBy("SentimentText") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# set seed for reproducibility
(trainingData, testData) = data.randomSplit([0.85, 0.15], seed=100)
print("Training Dataset Count: " + str(trainingData.count()))
print("Test Dataset Count: " + str(testData.count()))

print("Type of data",type(trainingData))

# regular expression tokenizer
regexTokenizer = RegexTokenizer(
    inputCol="SentimentText", outputCol="words", pattern="\\W")

# stop words
add_stopwords = ["http", "https", "amp", "rt", "t", "c", "the"]
stopwordsRemover = StopWordsRemover(
    inputCol="words", outputCol="filtered").setStopWords(add_stopwords)

# bag of words count
countVectors = CountVectorizer(
    inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)

# convert string labels to indexes
label_stringIdx = StringIndexer(inputCol="Sentiment", outputCol="label")

# lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0)
lr = NaiveBayes(smoothing=1.0, modelType="multinomial")
# lr = LinearSVC(maxIter=10, regParam=0.1)

#lrModel = lr.fit(trainingData)


# build the pipeline
pipeline = Pipeline(
    stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx, lr])

print("Mailstone1")
# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(trainingData)
predictions = pipelineFit.transform(testData)
print("Mailstone2")

predictions.filter(predictions['prediction'] == 0) \
    .select("SentimentText", "Sentiment", "probability", "label", "prediction") \
    .orderBy("probability", ascending=False) \
    .show(n=10, truncate=30)
print("Mailstone3")

predictions.filter(predictions['prediction'] == 1) \
    .select("SentimentText", "Sentiment", "probability", "label", "prediction") \
    .orderBy("probability", ascending=False) \
    .show(n=10, truncate=30)
print("Mailstone4")
# Evaluate, metricName=[accuracy | f1]default f1 measure
# evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction",labelCol="label")
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy: %g" % (accuracy))

# save the trained model for future use
pipelineFit.write().overwrite().save("piplineModel")

# PipelineModel.load("logreg.model")
