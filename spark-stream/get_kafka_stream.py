#!/usr/bin/python
from __future__ import print_function
import os
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import redis

redishost = 'ip-10-0-0-10'
redisport = xxxx
redispasswd= xxxx

# get list of driver records from the bulk query
def get_drivers(result):
    jsonResult = json.dumps(result)
    records = []
    ids = []
    for k, v in result.items():
        if str(k) == 'hits':
            for item in v['hits']:
                records.append(item['_source'])
                ids.append(item['_id'])
    return (records, ids)

	
# store multiple driver records in the elasticsearch using bulk write
def storeDrivers(rdd):
	cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	driverslist = []
	for x in rdd:
		driverslist.append({'_index':'driver','_type':'alldriver','_id':x[1],'_source':x[2]})

	helpers.bulk(es,driverslist)

#prepare and send driver bulk query to elastic search
def send_driver_bulk_query(senderlist):
	cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	if(len(senderlist)==0):
		return 'None'
	def create_query(doclist):
 
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

	return es.msearch(index='driver',search_type='query_and_fetch',body=querylist)

# helper function claim driver upon match
def secure_best_driver(driverList,r_db):
    id_idx = 0
    for driver in driverList:
        # print "Driver: "+ str(driver)
	if(secure_resource(driver['id'],'driver',r_db) == True):
            return (driver, id_idx)
        id_idx = id_idx + 1
    return ('None', -1)


#  for senders do bulk query for driver match
#  upon match store match in redis db
#  store sender record with bulk write api in elasticsearch

def storeSenders(rdd):
	cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	senderlist = []

	for x in rdd:
		senderlist.append(x[2])

	if(len(senderlist) >  0):
		res = send_driver_bulk_query(senderlist)

		if(res != 'None' and 'responses' in res):
			# print("QUERY LEN",len(senderlist))
			# print("RESPONSE LEN",len(res['responses']))
			# print(res)
			
			rs_driverdb = redis.StrictRedis(host=redishost,  port=redisport, db=1, password=redispasswd)
			rs_senderdb = redis.StrictRedis(host=redishost,  port=redisport, db=2, password=redispasswd)

			for i in range(len(res['responses'])):
				#for each qunery check if driver available claim in redis
				(drivers, dr_ids) = get_drivers(res['responses'][i])
				(selected_driver, id_idx) = secure_best_driver(drivers,rs_driverdb)
				if(selected_driver != 'None'):
					commit_resource(senderlist[i]['id'],'sender',rs_senderdb)
					# todo bulk update  driver
					#driverlist.append(selected_driver['id'])
					doc = {"doc": {"match": "true"}}
					ures=es.update(index='driver',doc_type='alldriver',id=selected_driver['id'],body=json.dumps(doc),ignore=[409])
					senderlist[i]['match']='true'


		def add_senderdb_header(doc):
			return {
				'_index': 'sender',
				'_type': 'allsender',
				'_id':doc['id'],
				'_source':json.dumps(doc) }



		senderlist = map(add_senderdb_header, senderlist)

		helpers.bulk(es,senderlist)

# secure driver or sender resource in redis
def secure_resource(id,type,r_db):
	redishost = 'ip-10-0-0-10'
	redisport = xxxx
	redispasswd=xxxx
 	ret_val=False

	# redis watch transaction ensures that 
	# the record we want to write if claimed by some else
	# we get notified so to avoid overbooking scenario
	
	pipe = r_db.pipeline()
	pipe.watch(id)
       	if (str(r_db.get(id)) == 'None'):
		pipe.multi()
		pipe.set(id, 1)
		try: 
			pipe.execute()
			ret_val = True
		except:
			ret_val = False

	return ret_val

# commit sender record
def commit_resource(id,type,r_db):
	redishost = 'ip-10-0-0-10'
	redisport = xxxx
	redispasswd=xxxx

    	r_db.set(id, 1)

# find senders. for now just store driver record
# let sender do the search. 
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

	match_found = "false"


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

	# print(json.dumps(driver_entry))

	return(1, x['id'],json.dumps(driver_entry))

'''
prepare sender entry
'''


def finddriver(x):

	senders = []
		
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
	

	match_found = "false"

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

	# print(json.dumps(sender_entry))

	return(1, x['id'],sender_entry)


# read driver and sender events from 2 seperate streams
# process and handoff for further processing
def main():
	"""Runs and specifies map reduce jobs for streaming data. Data
		is processed in 2 ways to be sent both to redis and ES"""
	print("in spark main")

	sc = SparkContext(appName="cargo")
	ssc = StreamingContext(sc, 5)
	sc.setLogLevel("WARN")    

	cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']
	redishost = 'ip-10-0-0-10'
	redisport = xxxx 
	redispasswd= xxxx
    	rs_driverdb = redis.StrictRedis(host=redishost,  port=redisport, db=1, password=redispasswd)
    	rs_senderdb = redis.StrictRedis(host=redishost,  port=redisport, db=2, password=redispasswd)

    	rs_driverdb.flushdb()
    	rs_senderdb.flushdb()


    	brokers = ','.join(['{}:9092'.format(i) for i in cluster])



    	driver = KafkaUtils.createDirectStream(ssc, ['DRIVER'], {'metadata.broker.list':brokers})

    	sender = KafkaUtils.createDirectStream(ssc, ['SENDER'], {'metadata.broker.list':brokers})


	D = driver.map(lambda x: json.loads(x[1])).map(findsenders)\
		.filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(storeDrivers))
	S = sender.map(lambda x: json.loads(x[1])).map(finddriver)\
		.filter(lambda x: x[0]==1).foreachRDD(lambda rdd: rdd.foreachPartition(storeSenders))

	print("SPARK STREAMING")
	ssc.start()
	ssc.awaitTermination()

if __name__ == '__main__':
	main()
