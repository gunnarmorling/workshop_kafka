package de.kafka;

import de.kafka.protocol.avro.location.SenderLocation;
import de.kafka.protocol.avro.user.SenderUser;
import de.kafka.protocol.generic.SenderString;
import de.kafka.protocol.json.SenderCar;
import de.kafka.protocol.json.types.Car;
import example.avro.Location;
import example.avro.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class MainProducer {

    private final SenderUser senderUser;
    private final SenderLocation senderLocation;
    private final SenderString senderString;
    private final SenderCar senderCar;

    @Autowired
    public MainProducer(SenderUser senderUser,
                        SenderLocation senderLocation,
                        SenderString senderString,
                        SenderCar senderCar) {
        this.senderUser = senderUser;
        this.senderLocation = senderLocation;
        this.senderString = senderString;
        this.senderCar = senderCar;
    }

    public static void main(String[] args) {

        ConfigurableApplicationContext run = SpringApplication.run(MainProducer.class, args);
        MainProducer mainProducer = run.getBean(MainProducer.class);

        User user = new User("Roger", 1, "green");
        Location location = new Location("Hamburg", 1);
        Car car = new Car("DeLorean DMC-12", "DeLorean Motor Company", "BTTF");

        Car car1 = new Car("DeLorean DMC-12", "DeLorean Motor Company", "BTTF");
        Car car2 = new Car("Esprit", "Lotus", "JB");
        Car car3 = new Car("DB 5", "Astom Martin", "JB");
        Car car4 = new Car("Dudu", "VW", "Movie");

        try {

            mainProducer.senderUser.send(user).get();
            mainProducer.senderLocation.send(location).get();
            mainProducer.senderString.send("hello World").get();
            mainProducer.senderCar.send(car).get();

            mainProducer.senderCar.sendCarPartitioned(car1).get();
            mainProducer.senderCar.sendCarPartitioned(car2).get();
            mainProducer.senderCar.sendCarPartitioned(car3).get();
            mainProducer.senderCar.sendCarPartitioned(car4).get();
            mainProducer.senderCar.sendCarPartitioned(car1).get();
            mainProducer.senderCar.sendCarPartitioned(car1).get();

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}
