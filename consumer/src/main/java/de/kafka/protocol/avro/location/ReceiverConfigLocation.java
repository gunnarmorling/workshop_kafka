package de.kafka.protocol.avro.location;

import de.kafka.protocol.avro.common.AvroDeserializer;
import de.kafka.common.ConfigUtil;
import example.avro.Location;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class ReceiverConfigLocation {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ConfigUtil configUtil;

    public ReceiverConfigLocation(ConfigUtil configUtil) {
        this.configUtil = configUtil;
    }

    private ConsumerFactory<String, Location> consumerFactoryLocation() {
        return new DefaultKafkaConsumerFactory<>(configUtil.makeConfig("location"),
            new StringDeserializer(),
            new AvroDeserializer<>(Location.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Location> kafkaListenerContainerFactoryLocation() {
        ConcurrentKafkaListenerContainerFactory<String, Location> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryLocation());

        return factory;
    }

}