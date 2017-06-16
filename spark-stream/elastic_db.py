from elasticsearch import Elasticsearch
import sys

cluster = ['ip-10-0-0-4', 'ip-10-0-0-7', 'ip-10-0-0-14', 'ip-10-0-0-8']

'''
 include_in_all means there is _all index search do you want the field to be included in that
 or no. for text search it is useful otherwise you can have it removed to make things more efficient
''' 


def create_indices():
	print("creating driver and sender indices")

	driver_mapping = {
	  'settings' : {
		'number_of_shards' : 2,
		'number_of_replicas' : 2
	  },
	  'mappings': {
	    'alldriver': {
	      'properties': {
		'id': {'type': 'integer'},
		'space': {'type': 'integer', "include_in_all": "false"},
		'price': {'type': 'integer', "include_in_all": "false"},
		'review': {'type': 'integer', "include_in_all": "false"},
		'sloc': {'type': 'geo_point'},
		'dloc': {'type': 'geo_point'},
		'stime': {'type': 'date'},
		'etime': {'type': 'date'},
		'dist': {'type': 'integer',"include_in_all":"false"},
		'match': {'type': 'string',"include_in_all":"false"},
	      }
	    }
	  }
	}


	sender_mapping = {
	  'settings' : {
		'number_of_shards' : 2,
		'number_of_replicas' : 2
	  },
	  'mappings': {
	    'allsender': {
	      'properties': {
		'id': {'type': 'integer'},
		'space': {'type': 'integer',"include_in_all":"false"},
		'sloc': {'type': 'geo_point'},
		'dloc': {'type': 'geo_point'},
		'stime': {'type': 'date'},
		'etime': {'type': 'date'},
		'review': {'type': 'integer', "include_in_all": "false"},
		'match': {'type': 'string',"include_in_all":"false"},
	      }
	    }
	  }
	}

	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	es.indices.create(index="driver",body=driver_mapping);
	es.indices.create(index="sender",body=sender_mapping);
	print("created driver and sender indices")

def delete_indices():
	print("deleting driver and sender indices")
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	es.indices.delete(index='driver',ignore=[404,400])
	es.indices.delete(index='sender',ignore=[404,400])
	print("deleted driver and sender indices")

def get_total_match():
	print("total matches")
	es = Elasticsearch(cluster, http_auth=('elastic','changeme'))
	q = {
  		'size':0,
  		'query': { 'term': {'match': 'true'} },
   		"aggs" : {
        		"avg_review" : { "avg" : { "field" : "review" } }
    		}
	    }
	res = es.search(index="driver", body=q)
	print(res['hits']['total'])
	print("Average review ", res['aggregations']['avg_review']['value'])
	q = {
  		'size':0,
  		'query': { 'term': {'match': 'true'} },
   		"aggs" : {
        		"avg_price" : { "avg" : { "field" : "price" } }
    		}
	    }
	res = es.search(index="driver", body=q)
	print("Average price per mile", res['aggregations']['avg_price']['value'])



def main():
    # print command line arguments
    if(len(sys.argv) == 2):
	if (sys.argv[1] == "add"):
		create_indices()
	elif(sys.argv[1] == "rem"):
		delete_indices()
	elif(sys.argv[1] == "match"):
		get_total_match()
	else:
		print("WRONG OPTIONS. PROVIDE add or rem option")
    else:
	print("PROVIDE add or rem as options")
	

if __name__ == "__main__":
    main()
