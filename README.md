# Special Delivery
Build Community One Package At A Time </br></br>
Here are the project slides along with demo </br>
</br>
[Project Slides](http://bit.ly/2s9L3SW)

This project is about getting public involved in shipping packages to improve convenience and service
* Today's shipping is expensive and hard to track missing/delayed packages. Its also not environment friendly as special purpose vehicles deliver packages to you
* There may be a person nearby who is going to the destination you want to send the package to and could deliver it for you.
* By involving public, we can reduce the cost of the transport, improve service and help environment Which is what Special Delivery project is set out to achieve!

# Key Challenges
* Event Throughput: Initial Event Throughput was 150 messages per second. 
* Then i tuned kafka logs and incremented the number of partitions from 2 to 6 as events were getting serialized before being processed in spark streaming context. This helped improve the throughput to about 300 messages per second.
* I also looked debugged the elastic-search event processing times in detail as for each driver/sender events the record was being written in elastic-search and also query was being done to find the nearest match. 
* Upon going through elasticsearch in detail i found i could use bulk interface where we could write records in bulk and read in bulk(in the spark streaming window timeframe). This helped throughput increment to over 800 messesages per second a very good imporvement over previous throughput
* OVERBOOKING </br>
  * When i was testing events over 6000 messages per second, i noticed that multiple senders(thorugh distributed spark cluster) could be matched up to the same driver which is incorrect and driver could get overbooked.
  * I tried to use elasticsearch update with query option. However elasticsearch was already proving to be a bottleneck and i came across that update interface is expensive. So I reasearched and came across Redis in memory database that i could use.
  * REDIS also provides a WATCH record system where a process can WATCH a record and make sure while it is writing the record if someone else already modified the record, then redis provides the exception notification for the same, and you could avoid matching with the same driver. This helped avoid overbooking and also helped improve performance as REDIS in memory database is very efficient for writing records
  
# Improvements for throughput and scale
With more time on the project, i would look at the following improvements
* Tune the indexing of elastic-search</br>
  * Elasticsearch does reverse indexing of the records to improve efficiency of query. We can turn-off fields that are not searched.
  * By default the index refresh time is 1 second which could be agressive. I would play with this time to a higher value at the same time, when bulk writes are made the indexing can be turned off and turned back on after the operation so that we are not blocked during bulk operation. 
* Increment the number of cores(threads) of spark, i had 2 workers, i could increase more and probably that will help aswell, and if required we can also use more powerful amazon instances(i used m2.large) as the scale increases.
* Play with spark streaming window time. I had time of 5 seconds. But if we could increase/decrease the window time if application timing requirements are okay to see if that helps improve throughput further
* Scale and partitioning: Make sure, with partitioning of Redis and Elastic search are providing proper distribution of load in the cluster so that our queries and writes are efficient and are happening in memory so that we can scale and expand dynamically as we scale up.
* Improve the efficiency of query by removing already matched queries(currently wasn't the focus for the duration of the 3 week project) by moving into another book keeping database instead of main database.
* Even consider another database such as POSTGRES which provides geosearch capability and could be more efficient. This could be a good option instead of elasticsearch as elasticsearch is efficient for textsearch and maynot be as efficient for non-text based search such as our application.
# Feature improvement ideas
Following feather improvements can be done. Some of the following were thought about for the project but were not implemented due to time constraints.
* Event generation:Currently the driver/sender events are generated based on random distribution for source/destination, price,review,distance(max distance driver would like to travel),time(time the package has to be delivered by). We can also use poisson distribution so that we can model driver/senders entering, leaving and reentering the system for more realistic scenario.
* Multipoint pickup: Currently the pickup and dropoff is point to point. We can make this multipoint where driver could dropoff/pickup packages at transit points along his destination. We may also use SPF algorithm like DIJKSTRA's algorithm to find best matched drivers.
* Reduce driving distance: One of the main goals of this platform is to reduce the driving distance a driver has to drive to pickup/dropoff packages. With elastic search currently geo-search is done by getting closest match to source and then these results are qualified by closest match to destination. This could have problem where the driver is closet to destination. This can be improved to get the match based on driver travelling minimum distance both and Source and destination
* We can implement further queries to support, best rated driver in a given area. Drivers by maximum trips/revenues. Driver's who are most active and others to monitor the various parameters.
* Efficient Review feedback: Currently the reviews are based on the 5 * system. To keep this type of platform more efficient we can implement text based review system. There may be a driver who did really good or a driver we damaged/stole the package and these can be captured in text based review system, and rating of such drivers can be made more appropriate according to the feedback they receive. We can run the text through deep learning NLP models based on LSTM(Long Short Term Memory) for sentiment analysis and also we can do classification such as driver is great/stole packages/delivered on time. This can be done on senders as well. This model can be run in real time for prediction and batch processing pipeline can be setup to train the model offline and keep the inline prediction model updated time to time to keep it latest and current. This will help improve the efficiency of platform in terms of user behavior and provide reward system. 

Also here is the link to my main github repo which lists some of my other porjects which also includes NLP based AI deeplearning model training.</br>

[Main Github Repo]  https://github.com/cosmos342







