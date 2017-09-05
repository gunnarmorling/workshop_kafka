package de.kafka.protocol.generic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReceiverString {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverString.class);

    @KafkaListener(topics = "${kafka.topic.generic.string}")
    public void receive(String data) {
        LOGGER.info("received string='{}'", data);
    }

   @KafkaListener(topics = "${kafka.topic.json.car}")
    public void receiveCarAsString(String data) {
        LOGGER.info("received json string='{}'", data);
    }

}
