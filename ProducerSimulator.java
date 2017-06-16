#package com.simulator.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.simulator.objects.ConnectedCar;

public class ProducerSimulator {

	private static final int NUM_CARS_TO_GENERATE = 10;

	private static double initial_latitude = 37.426645;
	private static double initial_longitude = -122.140925;
	private static String TOPIC_NAME = "connected_car_data_part4";

	public static void main(String[] args) {

		ConnectedCarSimulator simulator = new ConnectedCarSimulator();
		simulator.produceCarTrafficData();
	
		System.exit(0);

	}

	private static Properties createKafkaConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("broker.list", "localhost:9092");
		props.put("group.id", "None");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		return props;

	}

	public void produceCarTrafficData() {

		Producer<String, String> producer = new KafkaProducer<String, String>(createKafkaConfig());

		List<ConnectedCar> carList = generateCars();
		for (ConnectedCar car : carList) {

			producer.send(new ProducerRecord<String, String>(TOPIC_NAME, car.toString()));

		}
		producer.close();
		

	}

	private static List<ConnectedCar> generateCars() {

		//MAKE[] carMakeList = MAKE.values();
		List<ConnectedCar> simEventList = new ArrayList<ConnectedCar>();

		long start = System.currentTimeMillis();
		double latitude = initial_latitude;
		double longitude = initial_longitude;
		for (int i = 0; i < NUM_CARS_TO_GENERATE; i++) {

			ConnectedCar car = new ConnectedCar();
			car.setVin("VIN00000" + i);
			//MAKE make = carMakeList[0];
			car.setFuel_capacity(new Random().nextInt(60 - 20) + 20);

			Date eventTimestamp = new Date();
			double[] coordinates = generateLocation(initial_longitude, initial_latitude, 100000);
			longitude = coordinates[0];
			latitude = coordinates[1];
			car.setLatitude(latitude);
			car.setLongitude(longitude);
			car.setEventTimestamp(eventTimestamp);

			simEventList.add(car);
			System.out.println(car.toString());

		}
		long end = System.currentTimeMillis();
		
		Collections.shuffle(simEventList);
		System.out.println("Total time = " + (end - start) + " milli seconds");
		return simEventList;
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
		return new double[] { newLong, newLat };
	}

}
