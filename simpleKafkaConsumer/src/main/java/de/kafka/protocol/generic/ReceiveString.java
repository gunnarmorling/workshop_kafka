package de.kafka.protocol.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.kafka.common.ConfigUtil;
import de.kafka.protocol.json.ReceiverCar;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class ReceiveString implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverCar.class);
    private static final String TOPIC = "MyStringPartition";
    private final String id;

    private KafkaConsumer<String, String> consumer;

    public ReceiveString(String id) {
        this.id = id;
        final ConfigUtil configUtil = new ConfigUtil();
        consumer = new KafkaConsumer<>(configUtil.makeConfigString("CatchTheString"));
    }

    public void run() {

        try {

            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {

                    for (ConsumerRecord<String, String> record : records) {
                        logResponse(record);
                    }
                    commitAsync(consumer);
                }
            }

        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            LOGGER.info("SHUTDOWN string");
            consumer.close();
        }

    }

    public void shutdown() {
        consumer.wakeup();
    }

    private void logResponse(ConsumerRecord<String, String> record) {
        String data = String.format("topic = %s, partition = %s, offset = %d, key = %s, json = %s by %s",
            record.topic(), record.partition(), record.offset(), record.key(), record.value(), id);
        LOGGER.info(data);
    }

    private void commitAsync(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync((map, e) -> {
            consumer.assignment().forEach(t -> {
                OffsetAndMetadata oamd = map.get(t);
                String data = String.format("commiting offset = %d", oamd.offset());
                LOGGER.info(data);
            });

        });
    }
}
