import java.io.Serializable;
import java.util.Date;
import java.util.ArrayList;
import org.json.simple.JSONObject;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

class Location
{
	double latitude;
	double longitude;
	Location(double lat, double lon)
	{
		latitude=lat;
		longitude=lon;
	}

	@Override
	public String toString()
	{
		return latitude + "," + longitude;
	}
}

public class DriverSender implements Serializable{

	/**
	 * 
	 */

	private int id;
	// private Party partyType;
	private String type;
	// private ArrayList<Location> route;
	private Date startTime;
	private Date endTime;
	private Date eventTime;

	private int space;
	private int price;
	private Double slat;
	private Double slon;
	private Double elat;
	private Double elon;
	private int review;


	DriverSender(int id, int type,
			// ArrayList<Location> route, 
			Date startTime,
			Date endTime,
			Date eventTime,
			int space,
			int price,
			Double slat,
			Double slon,
			Double elat,
			Double elon,
			int review)
	{
		this.id = id;
		if(type == 0)
			this.type = "DRIVER";
		else
			this.type = "SENDER";
		// this.route = route;
		this.startTime = startTime;
		this.endTime = endTime;
		this.eventTime = eventTime;
		this.space = space;
		this.price = price;

		this.slat = slat;
		this.slon = slon;

		this.elat = elat;
		this.elon = elon;
		this.review = review;
	}
	
	// price

	public Date getEventTime() {
		return eventTime;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	
	@Override
	public String toString() {

  		JSONObject obj=new JSONObject();
  		obj.put("id",new Integer(id));
  		// obj.put("stime",new String(startTime.toString()));
		// obj.put("etime", new String(endTime.toString()));
  		obj.put("space",new Integer(space));
		if(type == "DRIVER")
  			obj.put("price",new Integer(price));
		if(type == "DRIVER")
			obj.put("review",new Integer(review));
		// obj.put("sloc", new String(" lat: " + slat + "," + "lon: " + slan + "")
		obj.put("slat", new Double(slat));
		obj.put("slon", new Double(slon));
		// obj.put("eloc", new String(" lat: " + elat + "," + "lon: " + elan + "")
		obj.put("dlat", new Double(elat));
		obj.put("dlon", new Double(elon));
		return obj.toString();

/*
		return  "{id:" + "" + id + "," +  "stime: " +  startTime + "," + "etime:" + endTime + "," + "space:" + space + "," + "price:" + price + "}";
*/
	}

	public static void main(String [] args)
	{
		System.out.println("DriverSender producer start ");
		EventProducer prod = new EventProducer("DRIVER","SENDER");
		if(args[0].equals("sr"))
			prod.prod_message(1);
		else if(args[0].equals("dr"))
			prod.prod_message(0);
		prod.close();
		System.out.println("EventProducer end");
	}
}


class EventProducer
{
	private String driver_topic_name;
	private String user_topic_name;
	private Producer<String, String> producer;

	EventProducer(String driver_topic_name, String user_topic_name)
	{
		this.producer = new KafkaProducer<String, String>(createKafkaConfig());
		this.driver_topic_name = driver_topic_name;
		this.user_topic_name = user_topic_name;
	}

	private static Properties createKafkaConfig() {
		Properties props = new Properties();
		// need to find what this localhost should be
		props.put("bootstrap.servers", "localhost:9092");
		props.put("broker.list", "localhost:9092");
		props.put("group.id", "None");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// TODO UNDERSTAND THIS
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		return props;
	}

	public void close()
	{
		producer.close();
	}

	public void prod_message(int type)
	{
		// sender
		Date timeStart = new Date();
		double slat = 37.441883;
		double slon = -122.143019;
		double elat = 37.368830;
		double elon = -122.036350;
		int space = 10;
		int price = 10;
		// Location l = new Location(lat,lon);
		// ArrayList<Location> al = new ArrayList<Location>();
		// al.add(l);
		if(type == 0)
		{
			System.out.println("PRODUCE msg to DRIVER");
			DriverSender dr = new DriverSender(0,type,/* al,*/timeStart,timeStart,timeStart,10,10,slat,slon,elat,elon,1);
			int i = 0;
			while(i < 1)
			{
				producer.send(new ProducerRecord<String,String>(this.driver_topic_name,dr.toString()));
				System.out.println("EventProducer prod_message DRIVER ");
				i++;
			}
		}
		else
		{
			// sender
			System.out.println("PRODUCE msg to SENDER");
			DriverSender dr = new DriverSender(1,type,/*al,*/timeStart,timeStart,timeStart,5,5,slat,slon,elat,elon,1);
			int i = 0;
			while(i < 1)
			{
				producer.send(new ProducerRecord<String,String>(this.user_topic_name,dr.toString()));
				System.out.println("EventProducer prod_message SENDER ");
				i++;
			}
		}
	}

	public static void main(String [] args)
	{
	}
}
