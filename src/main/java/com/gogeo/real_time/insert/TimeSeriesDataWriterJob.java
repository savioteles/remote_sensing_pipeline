package com.gogeo.real_time.insert;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.script.ScriptException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.gogeo.real_time.objects.TimeSeriesData;
import com.gogeo.utils.GeometryUtils;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class TimeSeriesDataWriterJob {
	public static void main(String[] args) throws IOException,
			InterruptedException, ScriptException {
		ParameterTool params = ParameterTool.fromArgs(args);
		String propertiesFile = params.getRequired("properties_file");
		Properties prop = new Properties();
		prop.load(new FileReader(propertiesFile));
		
		//kafka configurations
		String kafkaServers = prop.getProperty("kafka_servers");
		String topic = prop.getProperty("kafka_topic");
		
		//writer server configurations
		int numOfShards = Integer.parseInt(prop.getProperty("num_shards"));
		int shardsReplicasNum = Integer.parseInt(prop.getProperty("num_replicas"));
		String serverIp = prop.getProperty("server_ip");
		String clusterName = prop.getProperty("server_cluster_name");
		
		//insert configurations
		String layer = prop.getProperty("layer");
		String tiffFile = prop.getProperty("tiff_reference_file");
		
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "bfast_writer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		
		gdal.AllRegister();
		Dataset src = (Dataset) gdal.Open(tiffFile, gdalconst.GA_ReadOnly);
		double[] adfGeoTransform = new double[6];
		src.GetGeoTransform(adfGeoTransform);
		int rasterXSize = src.getRasterXSize();
		
		TimeSeriesDataWriter writer = new TimeSeriesDataWriter(serverIp, clusterName);
		writer.createDataLayer(layer, shardsReplicasNum, numOfShards);
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				String[] values = record.value().split("\n");
				for (String line: values) {
					List<TimeSeriesData> timeSeries = extractTimeSeries(line, rasterXSize, adfGeoTransform);
					writer.insertTimeSeriesData(layer, timeSeries);
				}
			}
		}
	}
	
	private static List<TimeSeriesData> extractTimeSeries(String line, int rasterXSize, double[] adfGeoTransform) {
		List<TimeSeriesData> tsList = new ArrayList<TimeSeriesData>();
		String[] split = line.split(",");
		int pixel = Integer.parseInt(split[0]);
		int xPixel = (pixel % rasterXSize) - 1;
		int yPixel = (int) Math
				.ceil(pixel / rasterXSize) - 1;
		Envelope bound = GeometryUtils.getPixelBound(adfGeoTransform, xPixel, yPixel);
		Geometry geom = GeometryUtils.toGeometry(bound);
		
		DateTimeFormatter pattern = DateTimeFormat.forPattern("yyyy-MM-dd");
		DateTime date = pattern.parseDateTime("2000-02-18");
		
		for (int i = 1; i < split.length; i++) {
			double pixelValue = Double.parseDouble(split[i]);
			
			//construct time series data
			TimeSeriesData tsData = new TimeSeriesData(xPixel, yPixel, geom, pixelValue, date.toDate());
			tsList.add(tsData);
			
			//time-series Modis satellite period
			date = date.plusDays(16);
		}
		
		return tsList;
	}
}
