package de.kafka.protocol.avro.location;

import de.kafka.common.ConfigUtil;
import example.avro.Location;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;


@Configuration
public class SenderConfigLocation {

    private final ConfigUtil configUtil;

    public SenderConfigLocation(ConfigUtil configUtil) {
        this.configUtil = configUtil;
    }

    private ProducerFactory<String, Location> producerFactory() {
        return new DefaultKafkaProducerFactory<>(configUtil.makeConfigAvro());
    }

    @Bean
    public KafkaTemplate<String, Location> kafkaTemplateLocation() {
        return new KafkaTemplate<>(producerFactory());
    }

}

