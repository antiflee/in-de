from app import app
from flask import jsonify

from app import app
from elasticsearch import Elasticsearch

cluster = ['ec2-35-162-170-87.us-west-2.compute.amazonaws.com']

@app.route('/')
@app.route('/index')
def index():
	return "Hello, World!"

@app.route('/matches')
def get_matches():
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
	jsonresponse = [{"MATCHED SENDERS": res1['hits']['total'] ,"AVG DRIVER REVIEW": res1['aggregations']['avg_review']['value'], "AVG PRICE PER MILE": res2['aggregations']['avg_price']['value']}]
	return jsonify(jsonresponse)
