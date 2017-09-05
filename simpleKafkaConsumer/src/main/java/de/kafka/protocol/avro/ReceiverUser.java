package de.kafka.protocol.avro;

import de.kafka.common.ConfigUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class ReceiverUser implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverUser.class);
    private static final String TOPIC = "MyUser";

    private KafkaConsumer<String, byte[]> consumer;

    public ReceiverUser() {
        final ConfigUtil configUtil = new ConfigUtil();
        consumer = new KafkaConsumer<>(configUtil.makeConfigByteArray("CatchTheUser"));
    }

    public void run() {

        try {

            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {

                ConsumerRecords<String, byte[]> records = consumer.poll(100);

                if (!records.isEmpty()) {

                    for (ConsumerRecord<String, byte[]> record : records) {
                        logResponse(record);
                        logUserName(record);
                    }

                    commitAsync(consumer);

                }
            }

        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            LOGGER.info("SHUTDOWN user");
            consumer.close();
        }

    }

    public void shutdown() {
        consumer.wakeup();
    }

    private void logUserName(ConsumerRecord<String, byte[]> record)  {
        GenericRecord genericRecord = null;
        try {
            genericRecord = deserialize(record.value());
            LOGGER.info(String.format("Deserialized name = %s", genericRecord.get("name")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private GenericRecord deserialize(byte[] record) throws IOException {

        final Schema schema = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\"," +
            "\"name\":\"User\"," +
            "\"namespace\":\"example.avro\"," +
            "\"fields\":[" +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"],\"default\":0}," +
            "{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"],\"default\":\"\"}]}");

        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(record, null);

        return datumReader.read(null, decoder);

    }

    private void logResponse(ConsumerRecord<String, byte[]> record) {
        String data = String.format("topic = %s, partition = %s, offset = %d, key = %s",
            record.topic(), record.partition(), record.offset(), record.key());
        LOGGER.info(data);
    }

    private void commitAsync(KafkaConsumer<String, byte[]> consumer) {
        consumer.commitAsync((map, e) -> {
            consumer.assignment().forEach(t -> {
                OffsetAndMetadata oamd = map.get(t);
                String data = String.format("commiting offset = %d for user", oamd.offset());
                LOGGER.info(data);
            });

        });
    }

}
