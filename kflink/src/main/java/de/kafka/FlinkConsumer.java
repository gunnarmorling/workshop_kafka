package de.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.IOException;
import java.util.Properties;

public class FlinkConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink_consumer");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(
            "MyCar", new SimpleStringSchema(), properties));

        stream.map(FlinkConsumer::extracManufacturer)
            .print();

        stream.map(FlinkConsumer::extracManufacturer)
            .map(String::length)
            .countWindowAll(4)
            .maxBy(0)
            .map (v -> "max length of last 4 manufacturers " + v)
            .print();

        stream.map(FlinkConsumer::extracManufacturer)
            .map(v -> 1)
            .timeWindowAll(Time.seconds(15))
            .sum(0)
            .map (v -> "Received in the last 15 seconds " + v)
            .print();

        env.execute();
    }

    private static String extracManufacturer(String value) {
        try {
            JsonNode jsonNode = MAPPER.readTree(value);
            return jsonNode.get("manufacturer").asText();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

}
