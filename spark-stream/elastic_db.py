from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys

#elastic db for storing driver/sender records and query in bulk
class ElasticDB:

	def __init__(self):
		cluster = ['ip-10-0-0-10', 'ip-10-0-0-7', 'ip-10-0-0-6', 'ip-10-0-0-8']
		self.es = Elasticsearch(cluster, http_auth=('elastic','changeme'))

	def store_bulk(self,inlist):
		helpers.bulk(self.es,inlist)
	
	def bulk_search(self,inindex,querylist):
		return self.es.msearch(index=inindex,search_type='query_and_fetch',body=querylist)

	def update_record(self,index_name,indoc_type, driver_id,doc):
		return self.es.update(index=index_name,doc_type=indoc_type,id=driver_id,body=doc,ignore=[409])

	#create driver index
	def create_indices(self):
		print("creating driver and sender indices")

		driver_mapping = {
		  'settings' : {
			'number_of_shards' : 10,
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
			'number_of_shards' : 10,
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

		self.es.indices.create(index="driver",body=driver_mapping);
		self.es.indices.create(index="sender",body=sender_mapping);
		# print("created driver and sender indices")

	# delete both driver and sender indices
	def delete_indices(self):
		print("deleting driver and sender indices")
		self.es.indices.delete(index='driver',ignore=[404,400])
		self.es.indices.delete(index='sender',ignore=[404,400])
		# print("deleted driver and sender indices")

	# get average review of matched drivers
	# get average price per mile
	def get_total_match(self):
		print("total matches")
		q = {
			'size':0,
			'query': { 'term': {'match': 'true'} },
			"aggs" : {
				"avg_review" : { "avg" : { "field" : "review" } }
			}
		    }
		res = self.es.search(index="driver", body=q)
		print(res['hits']['total'])
		print("Average review ", res['aggregations']['avg_review']['value'])
		q = {
			'size':0,
			'query': { 'term': {'match': 'true'} },
			"aggs" : {
				"avg_price" : { "avg" : { "field" : "price" } }
			}
		    }
		res = self.es.search(index="driver", body=q)
		print("Average price per mile", res['aggregations']['avg_price']['value'])


def main():
    # print command line arguments

	elastic_db = ElasticDB()
	if(len(sys.argv) == 2):
		if (sys.argv[1] == "add"):
			elastic_db.create_indices()
		elif(sys.argv[1] == "rem"):
			elastic_db.delete_indices()
		elif(sys.argv[1] == "match"):
			elastic_db.get_total_match()
		else:
			print("WRONG OPTIONS. PROVIDE add or rem or match option")
	else:
		print("PROVIDE add or rem as options")
	

if __name__ == "__main__":
    main()
