import java.io.Serializable;
import java.util.Date;
import java.util.ArrayList;
import java.util.Random;
import java.util.Collections;
import org.json.simple.JSONObject;

import java.lang.*;
import java.io.*;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.LocalTime;

// Stores Location based on lat and long
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

// Stores Driver or Sender information
public class DriverSender implements Serializable{

	private int id;
	private String type;
	// start time of event
	private LocalDateTime startTime;
	// end time of driver offer or senders time by which pkg to be delivered
	private LocalDateTime endTime;
	private LocalDateTime eventTime;

	private int space;
	private int price;
	// source lat/long
	private Double slat;
	private Double slon;
	// destination lat/long
	private Double elat;
	private Double elon;
	// driver or sender desired review
	private int review;
	private int distance;


	DriverSender(int id, int type,
			LocalDateTime startTime,
			LocalDateTime endTime,
			LocalDateTime eventTime,
			int space,
			int price,
			Double slat,
			Double slon,
			Double elat,
			Double elon,
			int review,
			int distance)
	{
		this.id = id;
		if(type == 0)
			this.type = "DRIVER";
		else
			this.type = "SENDER";
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
		this.distance = distance;
	}

	public String getType()
	{
		return type;
	}
	

	public LocalDateTime getEventTime() {
		return eventTime;
	}

	public void setEventTime(LocalDateTime eventTime) {
		this.eventTime = eventTime;
	}

	
	@Override
	public String toString() {
		String sTime = new String(startTime.toString()+"Z");
		String eTime = new String(startTime.toString()+"Z");

  		JSONObject obj=new JSONObject();
  		obj.put("id",new Integer(id));
  		obj.put("stime",sTime);
		obj.put("etime", eTime);
  		obj.put("space",new Integer(space));
		if(type == "DRIVER")
  			obj.put("price",new Integer(price));
		obj.put("review",new Integer(review));
		obj.put("slat", new Double(slat));
		obj.put("slon", new Double(slon));
		obj.put("dlat", new Double(elat));
		obj.put("dlon", new Double(elon));
		obj.put("dist", new Integer(distance));

		System.out.println(obj.toString());
		return obj.toString();
	}

	public static void main(String [] args)
	{
		System.out.println("DriverSender producer start ");
		if(args.length == 2)
		{
			String fname = args[0];
			EventProducer prod = new EventProducer("DRIVER","SENDER",fname);
			if(args[1].equals("sr"))
				prod.prod_message(1);
			else if(args[1].equals("dr"))
				prod.prod_message(0);
			else if(args[1].equals("blk"))
				prod.prod_bulk_messages();
			else	
				System.out.println("Please provide sr/dr or blk options");
			prod.close();
			System.out.println("EventProducer end");
		}
		else
		{
			System.out.println("Please provide filename and sr/dr/blk options");
		}
	}
}


// This class reads configuration from provided file and
// simulates Driver and Sender events and sends to Kafka 
// topic
class EventProducer
{
	private String driver_topic_name;
	private String sender_topic_name;
	private Producer<String, String> producer;
	private BufferedReader br;
	private	FileInputStream fstream = null;

	private int num_senders;
	private int num_drivers;

	private int reviewlow;
	private int reviewhigh;

	private int senderspacelow;
	private int senderspacehigh;

	private int driverspacelow;
	private int driverspacehigh;

	private int drivedistancelow;
	private int drivedistancehigh;

	private int radius;

	private int pricelow;
	private int pricehigh;
	private String [] brokers = new String[4];


	private Location [] locs = new Location[4];

	EventProducer(String driver_topic_name, String sender_topic_name, String fname)
	{
		this.driver_topic_name = driver_topic_name;
		this.sender_topic_name = sender_topic_name;

		try {
			fstream = new FileInputStream(fname);
		}
                catch(Exception e)
                {
                        System.err.println(e.getMessage());
                }


		try {
			br = new BufferedReader(new InputStreamReader(fstream));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}

		readConfig();
		this.producer = new KafkaProducer<String, String>(createKafkaConfig());

	}

	private void readConfig()
	{
		String strLine = null;
		String [] elements;
		Double lat;
		Double lon;

		System.out.println("reading configuration");

		while(true)
		{
			try {
				strLine = br.readLine();
			}
			catch(Exception e)
			{
				System.err.println(e.getMessage());
			}

			if(strLine == null)
				break;
			elements = strLine.split(",");

			if(elements[0].equals("num_senders"))
			{
				num_senders = Integer.parseInt(elements[1]);
				System.out.println("num_senders" + num_senders);
			}
			if(elements[0].equals("num_drivers"))
			{
				num_drivers =  Integer.parseInt(elements[1]);
				System.out.println("num_drivers" + num_drivers);
			}
			if(elements[0].equals("review"))
			{
				reviewlow = Integer.parseInt(elements[1]);
				reviewhigh = Integer.parseInt(elements[2]);
				System.out.println("reviewlow" + reviewlow + "reviewhigh" + reviewhigh);
			}
			if(elements[0].equals("senderspace"))
			{
				senderspacelow = Integer.parseInt(elements[1]);
				senderspacehigh = Integer.parseInt(elements[2]);
				System.out.println("senderspacelow" + senderspacelow + "senderspacehigh" + senderspacehigh);
			}
			if(elements[0].equals("driverspace"))
			{
				driverspacelow = Integer.parseInt(elements[1]);
				driverspacehigh = Integer.parseInt(elements[2]);
				System.out.println("driverspacelow" + driverspacelow + "driverspacehigh" + driverspacehigh);
			}
			if(elements[0].equals("drivedistance"))
			{
				drivedistancelow = Integer.parseInt(elements[1]);
				drivedistancehigh = Integer.parseInt(elements[2]);
				System.out.println("driverdistancelow" + drivedistancelow + "driverdistancehigh" + drivedistancehigh);
			}
			if(elements[0].equals("radius"))
			{
				radius = Integer.parseInt(elements[1]);
				System.out.println("radius" + radius);
			}
			if(elements[0].equals("price"))
			{
				pricelow = Integer.parseInt(elements[1]);
				pricehigh = Integer.parseInt(elements[2]);
				System.out.println("pricelow" + pricelow + "pricehigh" + pricehigh);
			}
			if(elements[0].equals("location1"))
			{
				locs[0] = new Location(Double.parseDouble(elements[1]),Double.parseDouble(elements[2]));
				// System.out.println("lat1 " + lat + " lon1 " + lon);
			}	
			if(elements[0].equals("location2"))
			{
				locs[1] = new Location(Double.parseDouble(elements[1]),Double.parseDouble(elements[2]));
				// System.out.println("lat2 " + lat + " lon2 " + lon);
			}	
	    		if(elements[0].equals("location3"))
			{
				locs[2] = new Location(Double.parseDouble(elements[1]),Double.parseDouble(elements[2]));
				// System.out.println("lat3 " + lat + "lon3 " + lon);
			}
	    		if(elements[0].equals("location4"))
			{
				locs[3] = new Location(Double.parseDouble(elements[1]),Double.parseDouble(elements[2]));
				// System.out.println("lat4 " + lat + " lon4 " + lon);
			}
			if(elements[0].equals("brokers"))
			{
				brokers[0] = new String(elements[1]);
				brokers[1] = new String(elements[2]);
				brokers[2] = new String(elements[3]);
				brokers[3]  = new String(elements[4]);
				System.out.println("brokers " + brokers[0] + brkers[1] + brokers[2] + brokers[3];
			}
	   
	    	}
	}

	private Properties createKafkaConfig() {
		Properties props = new Properties();
		// need to find what this localhost should be
		// String brokerslist = "ip-10-0-0-10:9092,ip-10-0-0-7:9092,ip-10-0-0-6:9092,ip-10-0-0-8:9092";
		String brokerslist = brokers[0]+":9092,"+ brokers[1]+":9092,"+brokers[2]+":9092," +brokers[3]+":9092"; 
		props.put("bootstrap.servers", brokerslist);
		props.put("broker.list", brokerslist);
		props.put("group.id", "None");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		return props;
	}

	public void close()
	{
		producer.close();
	}

    public static double[] generateLocation(double x0, double y0, int radius) {
                Random random = new Random();

                // Convert radius from meters to degrees
                double radiusInDegrees = radius / 111000f;

                double u = random.nextDouble();
                double v = random.nextDouble();
                double w = radiusInDegrees * Math.sqrt(u);
                double t = 2 * Math.PI * v;
                double x = w * Math.cos(t);
                double y = w * Math.sin(t);

                // Adjust the x-coordinate for the shrinking of the east-west distances
                double new_x = x / Math.cos(Math.toRadians(y0));

                double newLong = new_x + x0;
                double newLat = y + y0;
                // System.out.println("Longitude: " + foundLongitude + " Latitude: " +
                return new double[] { newLat, newLong };
     }

	private static LocalDateTime[]  getTimeRange()
	{
    	Random random = new Random();

		LocalDateTime timeStart = LocalDateTime.of(LocalDate.now(),
						LocalTime.of(random.nextInt(4), random.nextInt(60),
						random.nextInt(60), random.nextInt(999999999 + 1)));
		System.out.println("starttime" + timeStart);

		LocalDateTime timeEnd =  LocalDateTime.of(LocalDate.now(),
				LocalTime.of(16+random.nextInt(4), random.nextInt(60),
						random.nextInt(60), random.nextInt(999999999 + 1)));
		System.out.println("endtime" + timeEnd );

		return new LocalDateTime[] { timeStart, timeEnd };
	
	 }


	public void prod_bulk_messages()
	{
		ArrayList<DriverSender> drList = new ArrayList<DriverSender>();
		int srcIdx;
		int dstIdx;
		Date timeStart = new Date();

		for(int i = 0 ; i < num_senders ; i++)	
		{
			srcIdx = new Random().nextInt(4);	

			while((dstIdx = new Random().nextInt(4)) == srcIdx)
			{
				continue;
			} 
			LocalDateTime [] timeRange = getTimeRange();

			double [] srcLoc = generateLocation(locs[srcIdx].longitude,locs[srcIdx].latitude,radius);
			double [] dstLoc = generateLocation(locs[dstIdx].longitude,locs[dstIdx].latitude,radius);
			DriverSender dr = new DriverSender(i,1,timeRange[0],timeRange[1],LocalDateTime.now(),
								senderspacehigh,
								0,srcLoc[0],srcLoc[1],dstLoc[0],dstLoc[1],0,0);
			drList.add(dr);

		}
	
		for(int i = 0 ; i < num_drivers ; i++)	
		{
			srcIdx = new Random().nextInt(4);	
			while((dstIdx = new Random().nextInt(4)) == srcIdx)
			{
				continue;
			} 
			double [] srcLoc = generateLocation(locs[srcIdx].longitude,locs[srcIdx].latitude,radius);
			double [] dstLoc = generateLocation(locs[dstIdx].longitude,locs[dstIdx].latitude,radius);
			LocalDateTime [] timeRange = getTimeRange();

			DriverSender dr = new DriverSender(i,0,/* al,*/timeRange[0],timeRange[1],LocalDateTime.now(),
								driverspacehigh,
								new Random().nextInt(pricehigh-pricelow)+pricelow,srcLoc[0],srcLoc[1],dstLoc[0],dstLoc[1],new Random().nextInt(reviewhigh-reviewlow)+reviewlow,new Random().nextInt(drivedistancehigh-drivedistancelow)+drivedistancelow);
			drList.add(dr);

		} 

		long seed = System.nanoTime();
		Collections.shuffle(drList, new Random(seed));
		Collections.shuffle(drList, new Random(543440));
		Collections.shuffle(drList);
		for( DriverSender dr : drList)
		{
			if(dr.getType().equals("DRIVER"))
				producer.send(new ProducerRecord<String,String>(this.driver_topic_name,dr.toString()));
			else
				producer.send(new ProducerRecord<String,String>(this.sender_topic_name,dr.toString()));
		}
		System.out.println("prod_bulk_messages done");
		
	}

	// produces just 1 driver or sender event 
 	// mainly used for basic unit testing
	public void prod_message(int type)
	{
		// sender
		Date timeStart = new Date();
		double slat = 37.441883;
		double slon = -122.143019;
		double elat = 37.368830;
		double elon = -122.036350;
		int space; 
		int price;
		int review = 5;
		int distance = 50;

		System.out.print("PRODUCE msg to"); 
		if(type == 0)
		{
			System.out.println(" DRIVER");
			space = 10;
			price = 10;
		}
		else	
		{
			System.out.println(" SENDER");
			space = 5;
			price = 5;
		}

		LocalDateTime [] timeRange = getTimeRange();

		DriverSender ev = new DriverSender(type,type,timeRange[0],timeRange[1],LocalDateTime.now(),space,price,slat,slon,elat,elon,review,distance);
		producer.send(new ProducerRecord<String,String>((type == 0) ? this.driver_topic_name : this.sender_topic_name,ev.toString()));
		System.out.print("EventProducer send_message to ");
		if(type == 0)
			System.out.println("DRIVER");
		else
			System.out.println("SENDER");

	}

	public static void main(String [] args)
	{
	}
}
