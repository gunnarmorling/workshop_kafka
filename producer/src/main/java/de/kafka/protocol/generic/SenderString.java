package de.kafka.protocol.generic;

import de.kafka.protocol.avro.location.SenderLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SenderString {


    private static final Logger LOGGER = LoggerFactory.getLogger(SenderLocation.class);

    @Value("${kafka.topic.generic.string}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplateString;

    public SenderString(KafkaTemplate<String, String> kafkaTemplateString) {
        this.kafkaTemplateString = kafkaTemplateString;
    }

    public ListenableFuture<SendResult<String, String>> send(String data) {
        LOGGER.info("sending data='{}'", data);
        return kafkaTemplateString.send(topic, data);
    }
}
