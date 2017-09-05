package de.kafka.protocol.json;

import de.kafka.common.ConfigUtil;
import de.kafka.protocol.json.types.Car;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class SenderConfigCar {

    private final ConfigUtil configUtil;

    public SenderConfigCar(ConfigUtil configUtil) {
        this.configUtil = configUtil;
    }

    private ProducerFactory<String, Car> producerFactory() {
        return new DefaultKafkaProducerFactory<>(configUtil.makeConfigJson());
    }

    @Bean
    public KafkaTemplate<String, Car> kafkaTemplateCar() {
        return new KafkaTemplate<>(producerFactory());
    }
}
