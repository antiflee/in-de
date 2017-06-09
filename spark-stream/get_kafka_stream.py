from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils




def main():
	"""Runs and specifies map reduce jobs for streaming data. Data
		is processed in 2 ways to be sent both to redis and ES"""

	sc = SparkContext(appName="cargo")
	ssc = StreamingContext(sc, 30)
	sc.setLogLevel("WARN")    

    	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
    	brokers = ','.join(['{}:9092'.format(i) for i in cluster])

    	driver = KafkaUtils.createDirectStream(ssc, ['DRIVER'], {'metadata.broker.list':brokers})


	ssc.start()
	print("DONE SPARK STREAMING")
	ssc.awaitTermination()

if __name__ == '__main__':
	main()
