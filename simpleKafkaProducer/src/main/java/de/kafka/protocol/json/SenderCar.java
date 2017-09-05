package de.kafka.protocol.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.kafka.protocol.json.types.Car;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SenderCar {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderCar.class);
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("MyCar", 0);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static KafkaProducer<String, String> producer = makeKafkaStringProducer();

    public void send() throws ExecutionException, InterruptedException, JsonProcessingException {

        Car car = new Car("Aston Martin DB5", "Aston Martin", "JB");

        String jsonString = MAPPER.writeValueAsString(car);
        ProducerRecord<String, String> record =
            new ProducerRecord<>(TOPIC_PARTITION.topic(), car.getMake(), jsonString);

        LOGGER.info("sending car='{}'", car);
        producer.send(record).get();
    }

    private static KafkaProducer<String, String> makeKafkaStringProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", StringSerializer.class);
        return new KafkaProducer<>(kafkaProps);
    }

}
