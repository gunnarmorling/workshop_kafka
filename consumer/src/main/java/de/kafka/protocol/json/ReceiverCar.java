package de.kafka.protocol.json;

import de.kafka.protocol.json.types.Car;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class ReceiverCar {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverCar.class);

    @KafkaListener(topics = "${kafka.topic.json.car}", containerFactory = "kafkaListenerContainerFactoryCar")
    public void receive(Car car) {
        LOGGER.info("received car='{}'", car);
    }

    @KafkaListener(topics = "${kafka.topic.json.carPartition}", containerFactory = "kafkaListenerContainerFactoryCar")
    public void receive1(Car car,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                         Acknowledgment ack) {
        LOGGER.info("1: received car='{}' on partition {} on topic {}", car, partition, topic);
        ack.acknowledge();
    }

    @KafkaListener(topics = "${kafka.topic.json.carPartition}", containerFactory = "kafkaListenerContainerFactoryCar")
    public void receive2(Car car,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        LOGGER.info("2: received car='{}' on partition {} on topic {}", car, partition, topic);
    }

    @KafkaListener(topics = "${kafka.topic.json.carPartition}", containerFactory = "kafkaListenerContainerFactoryCar")
    public void receive3(Car car,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        LOGGER.info("3: received car='{}' on partition {} on topic {}", car, partition, topic);
    }

    @KafkaListener(topics = "${kafka.topic.json.carPartition}", containerFactory = "kafkaListenerContainerFactoryCar")
    public void receive4(Car car,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        LOGGER.info("4: received car='{}' on partition {} on topic {}", car, partition, topic);
    }


    @KafkaListener(topics = "${kafka.topic.json.carPartition}", containerFactory = "kafkaListenerContainerFactoryCar")
    public void receive5(Car car,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        LOGGER.info("5: received car='{}' on partition {} on topic {}", car, partition, topic);
    }


}
