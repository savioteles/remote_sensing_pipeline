package com.gogeo.experimental.flink.remote_sensing.writers;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import javax.script.ScriptException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CsvDataWriter {
	public static void main(String[] args) throws IOException,
			InterruptedException, ScriptException {
		ParameterTool params = ParameterTool.fromArgs(args);
		String kafkaServers = params.getRequired("kafka_servers");
		String topic = params.getRequired("kafka_topic");
		String output = params.getRequired("out_file");
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "bfast_writer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		
		while (true) {
			BufferedWriter writer = new BufferedWriter(new FileWriter(output));
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				String[] values = record.value().split("\n");
				for (String value: values) {
//					writer.write(value +"\n");
					System.out.println(value);
				}
			}
		}
	}
}
