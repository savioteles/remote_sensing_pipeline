package com.gogeo.experimental.flink.remote_sensing;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.gogeo.experimental.flink.remote_sensing.jobs.Bfast;

public class PipelineBFast {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String inputTopic = params.getRequired("topic");
        String scriptPath = params.getRequired("script");
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",
                params.get("kfk", "localhost:9092"));
        
        env.addSource(
                new FlinkKafkaConsumer010<byte[]>(inputTopic,
                        new RawSchema(), properties))
                .map(new Bfast(scriptPath))
                .addSink(
                        new FlinkKafkaProducer010<String>("out",
                                new SimpleStringSchema(), properties));

        env.setMaxParallelism(8).execute("Flink Streaming Experimental Stream Enrichment Pipeline (Java)");
    }

    private static class RawSchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {

        private static final long serialVersionUID = 1L;

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeExtractor.getForClass(byte[].class);
        }

        @Override
        public byte[] serialize(byte[] element) {
            return element;
        }

        @Override
        public byte[] deserialize(byte[] message) throws IOException {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }
    }
}
