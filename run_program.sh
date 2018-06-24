#!/bin/sh
python3 tweepy_stream.py & $SPARK_HOME/bin/spark-submit load_mo│
del.py >> out_model.txt