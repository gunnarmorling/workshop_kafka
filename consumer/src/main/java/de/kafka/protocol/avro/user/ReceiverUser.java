package de.kafka.protocol.avro.user;

import example.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReceiverUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverUser.class);

    @KafkaListener(topics = "${kafka.topic.avro.user}",  containerFactory = "kafkaListenerContainerFactoryUser")
    public void receive(User user) {
        LOGGER.info("received user='{}'", user.toString());
    }

}