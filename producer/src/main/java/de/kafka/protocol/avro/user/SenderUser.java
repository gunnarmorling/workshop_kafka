package de.kafka.protocol.avro.user;

import example.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SenderUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderUser.class);

    @Value("${kafka.topic.avro.user}")
    private String userTopic;

    private final KafkaTemplate<String, User> kafkaTemplateUser;

    public SenderUser(KafkaTemplate<String, User> kafkaTemplateUser) {
        this.kafkaTemplateUser = kafkaTemplateUser;
    }

    public ListenableFuture<SendResult<String, User>> send(User user) {
        LOGGER.info("sending user='{}'", user.toString());
        return kafkaTemplateUser.send(userTopic, user);
    }
}