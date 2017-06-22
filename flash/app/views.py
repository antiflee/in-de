from app import app
from flask import jsonify

from app import app
from elasticsearch import Elasticsearch

from flask import render_template
import json

cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']

@app.route('/')
@app.route('/index')
@app.route('/index.html')
def index():
	match = {'driver': [37.800956 , -122.437020 ], 
                 'sender': [37.79,  -122.437020]}
	return render_template("index.html", title='Special Delivery', user = match)


@app.route('/stats')
def get_stats():
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	q = {
  		'size':0,
  		'query': { 'term': {'match': 'true'} },
   		"aggs" : {
        		"avg_review" : { "avg" : { "field" : "review" } }
    		}
	    }
	res1 = es.search(index="driver", body=q)

	q = {
  		'size':0,
  		'query': { 'term': {'match': 'true'} },
   		"aggs" : {
        		"avg_price" : { "avg" : { "field" : "price" } }
    		}
	    }
	res2 = es.search(index="driver", body=q)
	'''
	jsonresponse = [{"MATCHED SENDERS": res1['hits']['total'] ,"AVG DRIVER REVIEW": res1['aggregations']['avg_review']['value'], "AVG PRICE PER MILE": res2['aggregations']['avg_price']['value']}]
	return jsonify(jsonresponse)
	'''
        user = {'match':res1['hits']['total'],'review':res1['aggregations']['avg_review']['value'],'price':res2['aggregations']['avg_price']['value']}

	'''
	return render_template("stats.html", title = 'STATS', user = user)
	'''
  	user = { 'nickname': 'Miguel' } # fake user
  	return render_template("bookCab.html", title = 'Home', user = user)


@app.route('/matches')
def get_matches():
	print("IN GET MATCHES")
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	print("IS connected to ES")

	matches = { 'size': 1,
	  'query' : { 'term' : { 'match' : 'true'}},
	  'sort': [{ 'stime': { 'order': 'desc'}}]
	}

	res1 = es.search(index="sender", body=matches)
	senderInfo = []
	if(res1['hits']['total']):
		senderInfo.append(res1['hits']['hits'][0]['_source']['sloc'])
		print("JSON",json.dumps(senderInfo))

	matches = { 'size': 5,
	  'query' : { 'term' : { 'match' : 'true'}},
	  'sort': [{ 'stime': { 'order': 'desc'}}]
	}

	res1 = es.search(index="driver", body=matches)
	driverInfo = []
	if(res1['hits']['total']):
		for i in res1['hits']['hits']:
			driverInfo.append(i['_source']['sloc'])
	print("res1 total drivers",res1['hits']['total'])

	print("JSON",json.dumps(driverInfo))
	return (json.dumps({'snd': senderInfo,'drv':driverInfo}))


