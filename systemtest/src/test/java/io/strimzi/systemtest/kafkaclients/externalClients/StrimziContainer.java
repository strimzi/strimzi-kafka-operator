package io.strimzi.systemtest.kafkaclients.externalClients;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.TimeUnit;

public class StrimziContainer extends GenericContainer<StrimziContainer> {

    public StrimziContainer() {
        super("strimzi/kafka" + ":" + "latest-kafka-2.5.0");

        this.withExposedPorts(9093);
        this.withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092");
    }
}
