package com.gogeo.experimental.flink.remote_sensing.loaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LoadData {
    public static void main(String[] args) throws IOException,
            InterruptedException, ScriptException {
    	ParameterTool params = ParameterTool.fromArgs(args);
    	String input = params.getRequired("rdata_input");
    	String kafkaServers = params.getRequired("kafka_servers");
    	String topic = params.getRequired("kafka_topic");
    	
        String output = "/tmp/data_pieces";
        File outputDir = new File(output);
        if(!outputDir.exists()) {
            outputDir.mkdirs();
        }

        String command = "Rscript files/load_data.r "
                + input + " " + output;
        Process child = Runtime.getRuntime().exec(command
                );

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        System.out.println("Generating input files...");
        long time = System.currentTimeMillis();
        int code = child.waitFor();
        System.out.println("Input files created in " +(System.currentTimeMillis() - time) +"ms");

        System.out.println("Sending input files to Kafka...");
        time = System.currentTimeMillis();
        switch (code) {
        case 0:
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            child.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains(output)) {
                	System.out.println(line);
                    sendFileToKafka(line.trim(), topic, producer);
                }
            }
            br.close();
            break;
        case 1:
            // Read the error stream then
            String message = IOUtils.toString(child.getErrorStream(), Charset.defaultCharset());
            System.err.println("ERRO: " + message);
        }
        System.out.println("Files sent to Kafka in " +(System.currentTimeMillis() - time) +"ms");
        producer.close();
    }
    
    private static void sendFileToKafka(String filePath, String topic, Producer<String, byte[]> producer) throws IOException {
        Path path = Paths.get(filePath);
        byte[] data = Files.readAllBytes(path);
        producer.send(new ProducerRecord<String, byte[]>(topic, data));
    }
}
