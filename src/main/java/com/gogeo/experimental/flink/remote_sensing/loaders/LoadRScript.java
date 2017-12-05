package com.gogeo.experimental.flink.remote_sensing.loaders;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.script.ScriptException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.renjin.repackaged.guava.base.Strings;

import com.gogeo.real_time.objects.Params;
import com.gogeo.real_time.objects.Script;

import serialization.SerializeServiceFactory;

public class LoadRScript {
    public static void main(String[] args) throws IOException,
            InterruptedException, ScriptException {
    	ParameterTool params = ParameterTool.fromArgs(args);
    	
    	int initIndex = params.getInt("init_index", -1);
    	int endIndex = params.getInt("end_index", -1);
    	String inputFile = params.get("input_file");
    	String outputDir = params.get("output_dir");
    	
    	File scriptFile = new File(params.getRequired("script_file"));
    	
    	String kafkaServers = params.getRequired("kafka_servers");
    	String topic = params.getRequired("kafka_topic");
    	
    	
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        
        if (initIndex >= 0 && endIndex >= 0 && !Strings.isNullOrEmpty(inputFile) && !Strings.isNullOrEmpty(outputDir)) {
        	//Each distributed job handle one piece of the input data
        	for (int i = initIndex; i <= endIndex; i++) {
        		Params scriptParams = new Params(i, i, inputFile, outputDir);
        		send(producer, scriptFile, scriptParams, topic);
        	}
        } else
        	send(producer, scriptFile, null, topic);
    	
        producer.close();
    }
    
    private static void send(Producer<String, byte[]> producer, File scriptFile, Params scriptParams, String topic) throws IOException {
    	Script script = new Script(scriptFile, scriptParams);
        
        byte[] object = SerializeServiceFactory.getObjectSerializer().serialize(script);
        producer.send(new ProducerRecord<String, byte[]>(topic, object));
        
        System.out.println("Script File sent to Kafka.");
    }
}
