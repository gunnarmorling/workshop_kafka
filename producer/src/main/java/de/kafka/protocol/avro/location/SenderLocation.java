package de.kafka.protocol.avro.location;

import example.avro.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SenderLocation {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderLocation.class);

    @Value("${kafka.topic.avro.location}")
    private String locationTopic;

    private final KafkaTemplate<String, Location> kafkaTemplateLocation;

    public SenderLocation(KafkaTemplate<String, Location> kafkaTemplateLocation) {
        this.kafkaTemplateLocation = kafkaTemplateLocation;
    }

    public ListenableFuture<SendResult<String, Location>> send(Location location) {
        LOGGER.info("sending location='{}'", location.toString());
        return kafkaTemplateLocation.send(locationTopic, location);
    }
}