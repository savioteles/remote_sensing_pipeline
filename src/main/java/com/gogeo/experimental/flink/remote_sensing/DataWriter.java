package com.gogeo.experimental.flink.remote_sensing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import javax.script.ScriptException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;

public class DataWriter {
	
	public static void main(String[] args) throws IOException,
			InterruptedException, ScriptException {
		ParameterTool params = ParameterTool.fromArgs(args);
		String kafkaServers = params.getRequired("kafka_servers");
		String topic = params.getRequired("kafka_topic");
		String output = params.getRequired("out_file");
		String maskFile = params.getRequired("mask_file");
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "bfast_writer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));

		try {
			gdal.AllRegister();
			Driver driver = gdal.GetDriverByName("GTiff");
			Dataset src = (Dataset) gdal.Open(maskFile, gdalconst.GA_ReadOnly);
			Dataset datesDataset = driver.CreateCopy(output +"_date.tif", src, new String[]{"COMPRESS=LZW"});
			Dataset slopesDataset = driver.CreateCopy(output +"_slope.tif", src, new String[]{"COMPRESS=LZW"});

			while (true) {
				int[] band_list = {1};
				int buf_type = gdalconst.GDT_Float64;
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					String[] values = record.value().split("\n");
					
					for(String value: values) {
						String[] valueSplit = value.split(" "); 
						int pixel = Integer.parseInt(valueSplit[0]);
						int xoff = (pixel % src.getRasterXSize()) - 1;
						int yoff = (int) Math
								.ceil(pixel / src.GetRasterXSize()) - 1;
						double[] date = { Double.parseDouble(valueSplit[1]) };
						double[] slope = { Double.parseDouble(valueSplit[2]) };
						datesDataset.WriteRaster(xoff, yoff, 1, 1, 1, 1,
								buf_type, date, band_list, 0);
						slopesDataset.WriteRaster(xoff, yoff, 1, 1, 1, 1,
								buf_type, slope, band_list, 0);
						System.out.println(xoff +"\t" +yoff);
					}
				}
				datesDataset.FlushCache();
				slopesDataset.FlushCache();
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}
}
