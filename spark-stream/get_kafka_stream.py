#!/usr/bin/python
from __future__ import print_function
import os
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from elasticsearch import helpers

offsetRanges = []


def extractEvent(x):
    p = {
        'id' : x['id'],
        'stime' : x['stime'],
        'etime' : x['etime'],
        'space' : x['space'],
        'price' : x['price'],
    }

def storeDrivers(rdd):
	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	for x in rdd:
		es.create(index='driver',doc_type='alldriver',id=x[1],body=x[2])

def storeSenders(rdd):
	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	for x in rdd:
		es.create(index='sender',doc_type='allsender',id=x[1],body=x[2])

def findsenders(x):

	drid = x['id']
	space  = x['space']
	price = x['price']
	review = x['review']

	slon = x['slon']
	slat = x['slat']

	dlon = x['dlon']
	dlat = x['dlat']

	etime = x['etime']

	dist = x['dist']

	count = 0
	  
	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))

	sender_query = {
		"from": 0, "size": 1,
		"query": {
		"bool" : {
			"must" : { "range": { "review" : {"lte": review }}},
			"must_not" : { "term" : { "match" : "true"}},
			"must" : { "range" : { "space" : {"lte": space }}},
			"must" : { "range": { "etime" : {"lte": etime  }}},
			"filter" : {
				"geo_distance" : {
					"distance" : "100km",
					"distance_type": "plane",
					"dloc" : {
						"lat" : dlat,
						"lon" : dlon
					}
				 }
			 },
			 "filter": {
				 "geo_distance" : {
					"distance" : dist,
					"distance_type": "plane",
					"sloc" : {
						"lat" : slat,
						"lon" : slon
					}
				 }
			  }
		   }
		},
		"sort": [
		  {"_geo_distance":
			 {
				"sloc": {
						"lat" : slat,
						"lon" : slon
				},
				"order": "asc",
				"unit": "km",
				"distance_type": "plane"
			 }
		  },
		  { "_geo_distance":
			{
				"dloc": {
						"lat" : dlat,
						"lon" : dlon
				},
				"order": "asc",
				"unit": "km",
				"distance_type": "plane"
			 }
		  },
		  { "review":  {"order":"desc"}},
		  { "space": {"order":"desc"}},
	   ],
	}



	res = es.search(index="sender",doc_type="allsender",body=sender_query)

	match_found = "false"
	if(res["hits"]["total"] == 1 ):
		print("found senders")
		count = 1
		match_found = "true"
		senderid = res['hits']['hits'][0]["_source"]["id"];
		doc = {"doc": {"match": "true"}}
		res=es.update(index='sender',doc_type='allsender',id=senderid,body=json.dumps(doc),ignore=[409])
		print("updated sender")


	'''
	for row in res["hits"]["hits"]:
		sender =  row["_source"]
		print(sender)
	'''


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
		'match': match_found
	}

	'''
	print(json.dumps(driver_entry))
	'''

	return(1, x['id'],json.dumps(driver_entry))

'''
 search in sender database
 find driver based on going from same src,dest
 time range
 not yet assigned
 space match
 based on price
 based on max destance from  src/destination
 based on review match
 update user and update the driver
'''

def finddriver(x):

	srid = x['id']
	space  = x['space']
	review = x['review']

	slon = x['slon']
	slat = x['slat']

	dlon = x['dlon']
	dlat = x['dlat']

	etime = x['etime']

	dist = x['dist']

	count = 0
	

	'''
 	for now store in the database
	'''

	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']


	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))

	print("created sender entry in elastic search")

	driver_query = {
		"from": 0, "size": 1,
		"query": {
		"bool" : {
			"must" : { "range": { "review" : {"gte": review }}},
			"must_not" : { "term" : { "match" : "true"}},
			"must" : { "range" : { "space" : {"gte": space }}},
			"must" : { "range": { "etime" : {"gte": etime  }}},
			"filter" : {
				"geo_distance" : {
					"distance" : "50km",
					"distance_type": "plane",
					"dloc" : {
						"lat" : dlat,
						"lon" : dlon
					}
				 }
			 },
			 "filter": {
				 "geo_distance" : {
					"distance" : dist,
					"distance_type": "plane",
					"sloc" : {
						"lat" : slat,
						"lon" : slon
					}
				 }
			  }
		   }
		},
		"sort": [
		  {"_geo_distance":
			 {
				"sloc": {
						"lat" : slat,
						"lon" : slon
				},
				"order": "asc",
				"unit": "km",
				"distance_type": "plane"
			 }
		  },
		  { "_geo_distance":
			{
				"dloc": {
						"lat" : dlat,
						"lon" : dlon
				},
				"order": "asc",
				"unit": "km",
				"distance_type": "plane"
			 }
		  },
		  { "review":  {"order":"desc"}},
		  { "space": {"order":"desc"}},
	   ],
	}


	res = es.search(index="driver",doc_type="alldriver",body=driver_query)

	match_found = "false"
	if(res["hits"]["total"]):
		print("found driver")
		count = 1
		match_found = "true"
		driverid = res['hits']['hits'][0]["_source"]["id"];
		doc = {"doc": {"match": "true"}}
		res=es.update(index='driver',doc_type='alldriver',id=driverid,body=json.dumps(doc),ignore=[409])
		print("updated driver")

	sender_entry = {
		'id': x['id'],
		'space': x['space'],

		'sloc': {'lat':  x['slat'], 'lon': x['slon']},
		'dloc': {'lat':  x['dlat'], 'lon': x['dlon']},
		'stime': x['stime'],
		'etime': x['etime'],
		'review': x['review'],
		'match': match_found
	}
	'''
	print(json.dumps(sender_entry))
	'''
	return(1, x['id'],json.dumps(sender_entry))

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
	ssc = StreamingContext(sc, 8)
	sc.setLogLevel("WARN")    

	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	print("sucks 1")
    	brokers = ','.join(['{}:9092'.format(i) for i in cluster])

	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))


    	driver = KafkaUtils.createDirectStream(ssc, ['DRIVER'], {'metadata.broker.list':brokers})

    	sender = KafkaUtils.createDirectStream(ssc, ['SENDER'], {'metadata.broker.list':brokers})


	D = driver.map(lambda x: json.loads(x[1])).map(findsenders)\
		.filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(storeDrivers))
	S = sender.map(lambda x: json.loads(x[1])).map(finddriver)\
		.filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(storeSenders))

	ssc.start()
	print("DONE SPARK STREAMING")
	ssc.awaitTermination()
	print("sucks 4")

if __name__ == '__main__':
	main()
