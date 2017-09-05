package de.kafka.protocol.json;

import de.kafka.protocol.avro.location.SenderLocation;
import de.kafka.protocol.json.types.Car;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SenderCar {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderLocation.class);

    @Value("${kafka.topic.json.car}")
    private String carTopic;

    @Value("${kafka.topic.json.carPartition}")
    private String carPartitionTopic;

    private final KafkaTemplate<String, Car> kafkaTemplateCar;

    public SenderCar(KafkaTemplate<String, Car> kafkaTemplateString) {
        this.kafkaTemplateCar = kafkaTemplateString;
    }

    public ListenableFuture<SendResult<String, Car>> send(Car car) {
        LOGGER.info("sending car='{}' to {}", car, carTopic);
        return kafkaTemplateCar.send(carTopic, car.getMake(), car);
    }

    public ListenableFuture<SendResult<String, Car>> sendCarPartitioned(Car car) {
        LOGGER.info("sending car='{}' to {}", car, carPartitionTopic);
        return kafkaTemplateCar.send(carPartitionTopic, car.getMake(), car);
    }

}
