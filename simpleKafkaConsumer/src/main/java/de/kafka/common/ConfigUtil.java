package de.kafka.common;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConfigUtil {

    public Properties makeConfigString(String groupId) {
        Properties props = makeBaseConfig();
        props.put("group.id", groupId);
        props.put("value.deserializer", StringDeserializer.class);
        return props;
    }

    public Properties makeConfigByteArray(String groupId) {
        Properties props = makeBaseConfig();
        props.put("group.id", groupId);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        return props;
    }

    private Properties makeBaseConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("enable.auto.commit", false);
        return props;
    }

}
