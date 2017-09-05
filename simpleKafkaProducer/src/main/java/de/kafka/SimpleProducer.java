package de.kafka;

import de.kafka.protocol.avro.SenderUser;
import de.kafka.protocol.json.SenderCar;

public class SimpleProducer {

    public static void main(String[] args) {

        try {
            for (int i = 0; i < 2; i ++) {
                new SenderCar().send();
                new SenderUser().send();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
