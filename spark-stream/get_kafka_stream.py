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



def findsenders(x):

	drid = x['id']
	space  = x['space']
	price = x['price']
	review = x['review']

	slon = x['slon']
	slat = x['slat']

	dlon = x['dlon']
	dlat = x['dlat']
	  
	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	'''
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	'''
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'),verify_certs=False)

	sender_query = {
		"from": 0, "size": 5,
		"query": {
		"bool" : {
			"must" : { "range" : { "space" : {"lte": space }}},
			"filter" : {
				"geo_distance" : {
					"distance" : "10km",
					"distance_type": "plane",
					"dloc" : {
						"lat" : dlat,
						"lon" : dlon
					}
				 }
			 },
			 "filter": {
				 "geo_distance" : {
					"distance" : "10km",
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
		  { "space": {"order":"desc"}},
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
	   ],
	}


	res = es.search(index="sender",doc_type="allsender",body=sender_query)
	if(res["hits"]["hits"]):
		print("found senders")
	else:
		print("did not find senders")


	for row in res["hits"]["hits"]:
		sender =  row["_source"]
		print(sender)


	driver_entry = {
		'id':  x['id'],
		'space': x['space'],
		'price': x['price'],
		'review':  x['review'],

		'sloc': {'lat':  x['slat'], 'lon': x['slon']},
		'dloc': {'lat':  x['dlat'], 'lon': x['dlon']},
	}

	res = es.create(index="driver",doc_type="alldriver",id=x['id'],body=driver_entry)

	return x

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

	sender_entry = {
		'id': x['id'],
		'space': x['space'],

		'sloc': {'lat':  x['slat'], 'lon': x['slon']},
		'dloc': {'lat':  x['dlat'], 'lon': x['dlon']}
	}

	'''
 	for now store in the database
	'''

	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	'''
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	'''
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'),verify_certs=False)
	es.create(index='sender',doc_type='allsender',id=x['id'],body=sender_entry)

	print("created sender entry in elastic search")
	'''
		driver_query = {
			"from": 0, "size": 5,
			"query": {
			"bool" : {
				"must" : { "range" : { "space" : {"gte": space }}},
				"filter" : {
					"geo_distance" : {
						"distance" : "10km",
						"distance_type": "plane"
						"dloc" : {
							"lat" : dlat,
							"lon" : dlon 
						}
					 }
				 }
				 "filter": { 
					 "geo_distance" : {
						"distance" : "10km",
						"distance_type": "plane"
						"dloc" : {
							"lat" : dlat,
							"lon" : dlon 
						}		
					 }
				  }
			   }
			},
			"sort": [
			  { "space": {"order":"desc"}},
			  {"_geo_distance":
				 { 
					"sloc":  {
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
					"dloc": : {
							"lat" : dlat,
							"lon" : dlon 
					},		
					"order": "asc",
					"unit": "km", 
					"distance_type": "plane" 
				 }
			  },
		   ],
		}
	'''
	return x

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

	cluster = ['ip-10-0-0-5', 'ip-10-0-0-6', 'ip-10-0-0-8', 'ip-10-0-0-10']
	print("sucks 1")
    	brokers = ','.join(['{}:9092'.format(i) for i in cluster])

	'''
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	'''
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'),verify_certs=False)

	print("sucks 2")

    	driver = KafkaUtils.createDirectStream(ssc, ['DRIVER'], {'metadata.broker.list':brokers})

    	sender = KafkaUtils.createDirectStream(ssc, ['SENDER'], {'metadata.broker.list':brokers})


	D = driver.map(lambda x: json.loads(x[1])).map(findsenders)
	S = sender.map(lambda x: json.loads(x[1])).map(finddriver)

	'''
	D = sender.map(lambda x: json.loads(x[1]))
	S = sender.map(lambda x: json.loads(x[1]))
	'''

	D.pprint()
	S.pprint()
	ssc.start()
	print("DONE SPARK STREAMING")
	ssc.awaitTermination()
	print("sucks 4")

if __name__ == '__main__':
	main()
