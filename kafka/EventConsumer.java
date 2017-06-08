import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Map;

public class EventConsumer {
   public static void main(String[] args) throws Exception {
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      //Kafka consumer configuration settings
      String topicName = args[0].toString();
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serializa-tion.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serializa-tion.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      // consumer.subscribe(Arrays.asList(topicName));
      consumer.subscribe("DRIVER","USER");
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
      int i = 0;
      
      while (true) {
         // ConsumerRecords<String, String> records = consumer.poll(100);
         Map<String, ConsumerRecords<String,String>> crecords = consumer.poll(100);
     	for(Map.Entry<String,ConsumerRecords<String,String>> entry : crecords.entrySet())
    	{
         	String key = entry.getKey();
         	ConsumerRecords<String,String> records = entry.getValue();
		System.out.println("figure it out " + records);
		System.out.println("so" + records );
		/*
         	for (ConsumerRecord<String, String> record : records)
		{
         		System.out.printf("offset = %d, key = %s, value = %s\n", 
             		record.offset(), record.key(), record.value());
		}
		*/
         	// System.out.printf("offset = %d, key = %s, value = %s\n", 
         	//   entrySet.key(), entrySet.value());
	}
      }
   }
}
