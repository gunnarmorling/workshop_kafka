package de.kafka.protocol.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.kafka.common.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class ReceiverCar implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverCar.class);
    private static final String TOPIC = "MyCar";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KafkaConsumer<String, String> consumer;

    public ReceiverCar() {
        final ConfigUtil configUtil = new ConfigUtil();
        consumer = new KafkaConsumer<>(configUtil.makeConfigString("CatchTheCar"));
    }

    public void run() {

        try {

            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {

                    for (ConsumerRecord<String, String> record : records) {
                        logResponse(record);
                        logCarManufacturer(record);
                    }
                    commitAsync(consumer);
                }
            }

        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            LOGGER.info("SHUTDOWN car");
            consumer.close();
        }

    }

    public void shutdown() {
        consumer.wakeup();
    }

    private static class Car {
        private String make;

        Car(String make) {
            this.make = make;
        }

        String getMake() {
            return make;
        }

        @Override
        public String toString() {
            return "Car{" +
                "make='" + make + '\'' +
                '}';
        }

    }

    private void logCarManufacturer(ConsumerRecord<String, String> record) {
        try {
            JsonNode jsonNode = MAPPER.readTree(record.value());
            ReceiverCar.Car car = new ReceiverCar.Car(jsonNode.get("manufacturer").asText());
            String data = String.format("Deserialized manufacturer = %s", car.getMake());
            LOGGER.info(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void logResponse(ConsumerRecord<String, String> record) {
        String data = String.format("topic = %s, partition = %s, offset = %d, key = %s, json = %s",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
        LOGGER.info(data);
    }

    private void commitAsync(KafkaConsumer<String, String> consumer) {

        consumer.commitAsync((map, e) -> {
            consumer.assignment().forEach(t -> {
                OffsetAndMetadata oamd = map.get(t);
                String data = String.format("commiting offset = %d car", oamd.offset());
                LOGGER.info(data);
            });

        });
    }
}
