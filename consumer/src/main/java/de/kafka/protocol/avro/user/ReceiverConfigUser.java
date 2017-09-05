package de.kafka.protocol.avro.user;

import de.kafka.protocol.avro.common.AvroDeserializer;
import de.kafka.common.ConfigUtil;
import example.avro.User;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class ReceiverConfigUser {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ConfigUtil configUtil;

    public ReceiverConfigUser(ConfigUtil configUtil) {
        this.configUtil = configUtil;
    }

    private ConsumerFactory<String, User> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(configUtil.makeConfig("user"),
            new StringDeserializer(),
            new AvroDeserializer<>(User.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, User> kafkaListenerContainerFactoryUser() {
        ConcurrentKafkaListenerContainerFactory<String, User> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

}