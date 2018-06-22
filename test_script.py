from pyspark import SparkContext

for x in range(5000):
    if x % 100 == 0:
        print(x)
