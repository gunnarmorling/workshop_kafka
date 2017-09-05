package de.kafka.protocol.avro.user;

import de.kafka.common.ConfigUtil;
import example.avro.User;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;


@Configuration
public class SenderConfigUser {

    private final ConfigUtil configUtil;

    public SenderConfigUser(ConfigUtil configUtil) {
        this.configUtil = configUtil;
    }

    private ProducerFactory<String, User> producerFactory() {
        return new DefaultKafkaProducerFactory<>(configUtil.makeConfigAvro());
    }

    @Bean
    public KafkaTemplate<String, User> kafkaTemplateUser() {
        return new KafkaTemplate<>(producerFactory());
    }

}

