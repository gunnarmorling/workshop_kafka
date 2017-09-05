package de.kafka;

import de.kafka.protocol.avro.ReceiverUser;
import de.kafka.protocol.generic.ReceiveString;
import de.kafka.protocol.json.ReceiverCar;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleConsumer {

    public static void main(String[] args) {

        ReceiverCar receiverCar = new ReceiverCar();
        ReceiverUser receiverUser = new ReceiverUser();

        ReceiveString receiverString1 = new ReceiveString("c1");
        ReceiveString receiverString2 = new ReceiveString("c2");
        ReceiveString receiverString3 = new ReceiveString("c3");

        ExecutorService executor = Executors.newFixedThreadPool(5);

        executor.execute(receiverCar);
        executor.execute(receiverUser);

        executor.execute(receiverString1);
        executor.execute(receiverString2);
        executor.execute(receiverString3);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                receiverCar.shutdown();
                receiverUser.shutdown();

                receiverString1.shutdown();
                receiverString2.shutdown();
                receiverString3.shutdown();

                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


    }


}
