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
  * REDIS also provides a WATCH record lock system where a process can WATCH a record and make sure while it is writing the record if someone else already modified the record, then redis provides the notification for the same, and you could avoid matching with the same driver. This helped avoid overbooking and also helped improve performance as REDIS in memory database is very efficient for writing records

