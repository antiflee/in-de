from __future__ import print_function
'''
import redis, math, datetime, yaml
'''
import os
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
'''
from elasticsearch import Elasticsearch
from elasticsearch import helpers
'''

#################################################################
# To submit the job:
#
# park-submit --master local[*] --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --executor-memory 1G y.py
# spark-submit --master spark://ec2-54-71-28-156.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 y.py
# yarn application -kill APPID
# SPARK_HOME_DIR/bin/spark-submit --master spark://ec2-54-71-28-156.us-west-2.compute.amazonaws.com:7077 --kill $DRIVER_ID
#################################################################


offsetRanges = []

def extractEvent(x):
    p = {
        'id' : x['id'],
        'stime' : x['stime'],
        'etime' : x['etime'],
        'space' : x['space'],
        'price' : x['price'],
    }


def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print("%s %s %s %s",o.topic,o.partition,o.fromOffset,o.untilOffset)


def main():
	"""Runs and specifies map reduce jobs for streaming data. Data
		is processed in 2 ways to be sent both to redis and ES"""
	print("sucks")

	sc = SparkContext(appName="cargo")
	ssc = StreamingContext(sc, 5)
	sc.setLogLevel("WARN")    

	print("sucks 1")
    	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
    	brokers = ','.join(['{}:9092'.format(i) for i in cluster])

	print("sucks 2")

    	driver = KafkaUtils.createDirectStream(ssc, ['DRIVER'], {'metadata.broker.list':brokers})

	D = driver.map(lambda x: json.loads(x[1]))
	D.pprint()
	ssc.start()
	print("DONE SPARK STREAMING")
	ssc.awaitTermination()
	print("sucks 4")

if __name__ == '__main__':
	main()
