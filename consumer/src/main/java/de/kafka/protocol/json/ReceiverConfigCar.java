package de.kafka.protocol.json;

import de.kafka.common.ConfigUtil;
import de.kafka.protocol.json.types.Car;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class ReceiverConfigCar {


    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ConfigUtil configUtil;

    public ReceiverConfigCar(ConfigUtil configUtil) {
        this.configUtil = configUtil;
    }

    private ConsumerFactory<String, Car> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(configUtil.makeConfig("jsonCar"),
            new StringDeserializer(),
            new JsonDeserializer<>(Car.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Car> kafkaListenerContainerFactoryCar() {
        ConcurrentKafkaListenerContainerFactory<String, Car> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}
