package de.kafka.protocol.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SenderUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderUser.class);
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("MyUser", 0);
    private static KafkaProducer<String, byte[]> producer = makeKafkaByteArrayProducer();

    public void send() throws ExecutionException, InterruptedException {

        final Schema schema = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\"," +
            "\"name\":\"User\"," +
            "\"namespace\":\"example.avro\"," +
            "\"fields\":[" +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"],\"default\":0}," +
            "{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"],\"default\":\"\"}]}");

        GenericRecord user = new GenericData.Record(schema);
        user.put("name", "Alyssa");
        user.put("favorite_number", 256);
        user.put("favorite_color", "green");

        byte[] userBytes = makeByteArray(user, schema);

        LOGGER.info("sending user='{}'", user);

        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(TOPIC_PARTITION.topic(), userBytes);

        producer.send(record).get();

    }

    private byte[] makeByteArray(GenericRecord user, Schema schema) {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder =
            EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try {
            datumWriter.write(user, binaryEncoder);

            binaryEncoder.flush();
            byteArrayOutputStream.close();

            return byteArrayOutputStream.toByteArray();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        return new byte[0];
    }

    private static KafkaProducer<String, byte[]> makeKafkaByteArrayProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", ByteArraySerializer.class);
        kafkaProps.put("value.serializer", ByteArraySerializer.class);
        return new KafkaProducer<>(kafkaProps);
    }
}
