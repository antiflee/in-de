# Special Delivery
Build Community One Package At A Time </br></br>
Here are the project slides along with demo </br>
</br>
[Project Slides](http://bit.ly/2s9L3SW)

This project is about getting public involved in shipping packages to improve convenience and service
* Today's shipping is expensive and hard to track missing/delayed packages. Its also not environment friendly as special purpose vehicles deliver packages to you
* There may be a person nearby who is going to the destination you want to send the package to and could deliver it for you.
* By involving public, we can reduce the cost of the transport, improve service and help environment Which is what Special Delivery project is set out to achieve!

# Source Code Structure
 * KAFKA directory has DriverSender.java for simulating driver/sender events
 * spark-stream directory has get_kafka_stream.py for processing events in SPARKSTREAMING context and interaction with elasticsearch and redis
 * spark-stream directory  has elastic_db.py wrapper class to access ELASTICSEARCH
 * spark-stream directory  has redis_db.py- wrapper class to access REDIS
 * flash/app has front end flask web ui code

# Key Challenges
* Event Throughput: Initial Event Throughput was 150 messages per second. 
* Then i tuned kafka logs and increased the number of partitions as events were getting serialized before being processed in spark streaming context. This helped improve the throughput to about 300 messages per second.
* I also looked at the elastic-search event processing times in detail as for each driver/sender events the record was being written in elastic-search and also query was being done to find the nearest match. 
* Upon going through elasticsearch in detail i found i could use bulk interface where we could write records in bulk and query in bulk(in the spark streaming window timeframe). This helped as throughput improved to over 800, messesages per second an acceptable throughput for this type of application to begin with
* OVERBOOKING </br>
  * When i was testing events over 6000 messages per second, i noticed that multiple senders(thorugh distributed spark cluster) could be matched up to the same driver which is incorrect and driver could get overbooked.
  * I tried to use elasticsearch update with query option. However elasticsearch was already proving to be a bottleneck and i came across that update interface is expensive. So I reasearched and came across Redis in memory database that i could use.
  * REDIS also provides a WATCH record system where a process can WATCH a record and make sure while it is writing the record if someone else already modified the record, then redis provides the exception notification for the same, and you could avoid matching with the same driver. This helped avoid overbooking and also helped improve performance as REDIS in memory database is very efficient for writing records
  
# Improvements for throughput and scale
With more time on the project, i would look at the following improvements
* Tune the indexing of elastic-search</br>
  * Elasticsearch does reverse indexing of the records to improve efficiency of query. We can turn-off fields that are not searched.
  * By default the index refresh time is 1 second which could be agressive. I would play with this time to a higher value at the same time, when bulk writes are made the indexing can be turned off and turned back on after the operation so that we are not blocked during bulk operation. 
* Scale and partitioning: Make sure, with partitioning of Redis and Elastic search are providing proper distribution of load in the cluster so that our queries and writes are efficient and are happening in memory so that we can scale and expand dynamically as we scale up.
* I would also tune the number of replication entries for the database  and play with failure of nodes to make sure, service is uninterrupted and fix any issues seen

# Feature improvement ideas
Following  improvements can be done. Some of the following were thought about for the project but were not implemented due to time constraints.
* Batch Processing and More Aggregation Queries:We can implement further queries to support, best rated driver in a given area. Drivers by maximum trips/revenues. Driver's who are most active and others to monitor the various parameters. We can also store the matched/unmatched records in a record keeping databse and do batch processing of  older data to comeup with statistics. For providing per driver/sender statistics based on timeline CASSANDRA would be a good choice.
* Efficient Review feedback: Currently the reviews are based on the 5 * rating system. To keep this type of platform more efficient, we can implement text based review system. There may be a driver who did really good or a driver who damaged/stole the package and these can be captured in text based review system, and rating of such drivers can be made more appropriate according to the feedback they receive. We can run the text through deep learning NLP models based on LSTM(Long Short Term Memory)  and  do classification such as driver is great/not great/delivered on time, package classification as damaged packages/unaffected etc. Review classifications can be incorported on senders as well. These models can be run in real time for prediction. Batch processing pipeline can be setup to train the model offline and keep the inline prediction model updated time to time to keep it latest and current. This will help improve the efficiency of platform in terms of user behavior. 

Also here is the link to my main github repo which lists some of my other projects which also includes NLP based AI deeplearning model training.</br>

[Sandeep Bhat Github Repo]  https://github.com/cosmos342







