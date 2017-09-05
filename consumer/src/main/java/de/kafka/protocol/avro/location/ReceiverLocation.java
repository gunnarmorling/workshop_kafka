package de.kafka.protocol.avro.location;

import example.avro.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReceiverLocation {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverLocation.class);

    @KafkaListener(topics = "${kafka.topic.avro.location}", containerFactory = "kafkaListenerContainerFactoryLocation")
    public void receive(Location location) {
        LOGGER.info("received location='{}'", location);
    }

}