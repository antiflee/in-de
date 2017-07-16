#!/usr/bin/python
from __future__ import print_function
import os
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import argparse
from elastic_db import ElasticDB
from redis_db import RedisDB


#decode driver information
def decode_driver(self,x):

	driver_entry = {
		'id':  x['id'],
		'space': x['space'],
		'price': x['price'],
		'review':  x['review'],
		'sloc': {'lat':  x['slat'], 'lon': x['slon']},
		'dloc': {'lat':  x['dlat'], 'lon': x['dlon']},
		'stime': x['stime'],
		'etime': x['etime'],
		'dist': x['dist'],
		'match': 'false'
	}

	# print(json.dumps(driver_entry))

	return(1, x['id'],json.dumps(driver_entry))

# store multiple driver records in the elasticsearch using bulk write
def store_drivers(rdd):
	driverslist = []
	for x in rdd:
		driverslist.append({'_index':'driver','_type':'alldriver','_id':x[1],'_source':x[2]})

	elastic_db = ElasticDB()
	elastic_db.store_bulk(driverslist)

#decode sender information
def decode_sender(x):

	sender_entry = {
		'id': x['id'],
		'space': x['space'],

		'sloc': {'lat':  x['slat'], 'lon': x['slon']},
		'dloc': {'lat':  x['dlat'], 'lon': x['dlon']},
		'stime': x['stime'],
		'etime': x['etime'],
		'review': x['review'],
		'match': 'false'
	}

	# print(json.dumps(sender_entry))

	return(1, x['id'],sender_entry)


#prepare and send driver bulk query to elastic search
def send_driver_bulk_query(self,senderlist,elastic_db):
	if(len(senderlist)==0):
		return 'None'

	def create_query(self,doclist):
 
		res = []

		for doc in doclist:

			driver_query = {
				"from": 0, "size": 1,
				"query": {
				"bool" : {
					"must" : { "range": { "review" : {"gte": doc['review'] }}},
					"must_not" : { "term" : { "match" : "true"}},
					"must" : { "range" : { "space" : {"gte": doc['space'] }}},
					"must" : { "range": { "etime" : {"gte": doc['etime']  }}},
					"filter" : {
						"geo_distance" : {
							"distance" : "100km",
							"distance_type": "plane",
							"dloc" : {
								"lat" : doc['dloc']['lat'],
								"lon" : doc['dloc']['lon']
							}
						 }
					 },
					 "filter": {
						 "geo_distance" : {
							"distance" : "100km",
							"distance_type": "plane",
							"sloc" : {
								"lat" : doc['sloc']['lat'],
								"lon" : doc['sloc']['lon']
							}
						 }
					  }
				   }
				},
				"sort": [
				  {"_geo_distance":
					 {
						"sloc": {
								"lat" : doc['sloc']['lat'],
								"lon" : doc['sloc']['lon']
						},
						"order": "asc",
						"unit": "km",
						"distance_type": "plane"
					 }
				  },
				  { "_geo_distance":
					{
						"dloc": {
								"lat" : doc['dloc']['lat'],
								"lon" : doc['dloc']['lon']
						},
						"order": "asc",
						"unit": "km",
						"distance_type": "plane"
					 }
				  },
				  { "review":  {"order":"desc"}},
				  { "price":  {"order":"asc"}},
				  { "space": {"order":"desc"}},
			   ],
			}
			res.append({})
			res.append(driver_query)
		return res
			

		querylist = create_query(senderlist)
		# print("len of bulk query", len(querylist)) 
		# print("bulk query", querylist) 

		return elastic_db.bulk_search('driver',querylist)


# get list of driver records from the bulk query
def get_drivers_util(result):
	jsonResult = json.dumps(result)
	records = []
	ids = []
	for k, v in result.items():
		if str(k) == 'hits':
			for item in v['hits']:
				records.append(item['_source'])
				ids.append(item['_id'])
	return (records, ids)


#  for senders do bulk query for driver match
#  upon match store match in redis db
#  store sender record with bulk write api in elasticsearch
def store_senders(rdd):
	senderlist = []

	elastic_db = ElasticDB()
	redis_db =  RedisDB()
	for x in rdd:
		senderlist.append(x[2])

	if(len(senderlist) >  0):
		res = send_driver_bulk_query(senderlist,elastic_db)

		if(res != 'None' and 'responses' in res):
			# print("QUERY LEN",len(senderlist))
			# print("RESPONSE LEN",len(res['responses']))
			# print(res)

			for i in range(len(res['responses'])):
				#for each qunery check if driver available claim in redis
				(drivers, dr_ids) = get_drivers_util(res['responses'][i])
				(selected_driver, id_idx) = redis_db.secure_best_driver(drivers)
				if(selected_driver != 'None'):
					redis_db.commit_sender(senderlist[i]['id'])
					# todo bulk update  driver
					#driverlist.append(selected_driver['id'])
					doc = {"doc": {"match": "true"}}
					elastic_db.update_record('driver','alldriver',selected_driver['id'],json.dumps(doc))
					senderlist[i]['match']='true'


		def add_senderdb_header(self,doc):
			return {
				'_index': 'sender',
				'_type': 'allsender',
				'_id':doc['id'],
				'_source':json.dumps(doc) }



		senderlist = map(add_senderdb_header, senderlist)

		elastic_db.store_bulk(es,senderlist)


# read driver and sender events from 2 seperate streams
# process and handoff for further processing
def main():
	"""Runs and specifies map reduce jobs for streaming data. Data
		is processed in 2 ways to be sent both to redis and ES"""
	print("in spark main")
	parser = argparse.ArgumentParser(description='SparkStreaming Parser')
	parser.add_argument('--window', type=int,default=5, help='spark window size')
	
	parser=parser.parse_args()

	rdb = RedisDB()
	rdb.flush_db()


	sc = SparkContext(appName="SpecialDelivery")
	ssc = StreamingContext(sc,parser.window)
	sc.setLogLevel("WARN")

	# connect to driver and sender partitions
	cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']
	brokers = ','.join(['{}:9092'.format(i) for i in cluster])
 	driver = KafkaUtils.createDirectStream(ssc, ['DRIVER'], {'metadata.broker.list':brokers})
   	sender = KafkaUtils.createDirectStream(ssc, ['SENDER'], {'metadata.broker.list':brokers})

	
	D = driver.map(lambda x: json.loads(x[1])).map(decode_driver)\
		.filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(store_drivers))
	S = sender.map(lambda x: json.loads(x[1])).map(decode_sender)\
		.filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(store_senders))

	print("STREAMING")
	ssc.start()
	ssc.awaitTermination()



if __name__ == '__main__':
	main()
