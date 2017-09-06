package com.gogeo.experimental.flink.remote_sensing.loaders;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

import javax.script.ScriptException;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LoadCsvData {
    public static void main(String[] args) throws IOException,
            InterruptedException, ScriptException {
    	ParameterTool params = ParameterTool.fromArgs(args);
    	String input = params.getRequired("csv_data_directory");
    	int blockSize = Integer.parseInt(params.getRequired("block_size"));
    	String kafkaServers = params.getRequired("kafka_servers");
    	String topic = params.getRequired("kafka_topic");
    	
        File dir = new File(input);
        if (!dir.isDirectory()) {
        	System.err.println("The path " +input +" must be a directory.");
        	System.exit(1);
        }
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        
        
        long time = System.currentTimeMillis();
        System.out.println("Sending input files to Kafka...");
        
        //list txt files
        FileFilter fileFilter = new WildcardFileFilter("*.txt");
        String lineToSend = "";
        int localBlockSize = 0;
        
        for (File file: dir.listFiles()) {
        	System.out.println("Loading file " +file.getName());
	        BufferedReader br = new BufferedReader(
	                new FileReader(file));
	        String line;
	        
	        while ((line = br.readLine()) != null) {
	        	
	        	localBlockSize++;
	        	if (localBlockSize >= blockSize) {
	        		System.out.println("Sending " +localBlockSize +" ndvi time series ");
	        		lineToSend += line.trim();
	        		sendToKafka(lineToSend, topic, producer);
	        		lineToSend = "";
	        		localBlockSize = 0;
	        	} else
	        		lineToSend += line.trim() +"\n";
	        }
	        
	        br.close();
        }
        
        if(!lineToSend.isEmpty()) {
        	System.out.println("Sending remaining " +localBlockSize +" ndvi time series ");
        	sendToKafka(lineToSend, topic, producer);
        }
        System.out.println("Time to send: " +(System.currentTimeMillis() - time));
        
        producer.close();
    }
    
    private static void sendToKafka(String output, String topic, Producer<String, byte[]> producer) throws IOException {
    	String tmpFile = "/tmp/" +UUID.randomUUID().toString() +".csv";
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile));
        writer.write(output);
        writer.close();
        
        //get file byte array
        Path path = Paths.get(tmpFile);
        byte[] data = Files.readAllBytes(path);
        
        //send file to kafka
        producer.send(new ProducerRecord<String, byte[]>(topic, data));
        
        Files.deleteIfExists(path);
    }
}
