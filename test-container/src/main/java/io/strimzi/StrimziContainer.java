/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StrimziContainer extends GenericContainer<StrimziContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziContainer.class);

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final int KAFKA_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;

    private int kafkaExposedPort;
    private StringBuilder advertisedListeners;

    public StrimziContainer(final String version) {
        super("strimzi/kafka:" + version);
        super.withNetwork(Network.SHARED);

        // exposing kafka port from the container
        withExposedPorts(KAFKA_PORT);
    }

    public StrimziContainer() {
        this("latest-kafka-2.5.0");
    }

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        kafkaExposedPort = getMappedPort(KAFKA_PORT);

        LOGGER.info("This is mapped port {}", kafkaExposedPort);

        advertisedListeners = new StringBuilder(getBootstrapServers());

        Collection<ContainerNetwork> cns = containerInfo.getNetworkSettings().getNetworks().values();

        for (ContainerNetwork cn : cns) {
            advertisedListeners.append("," + "BROKER://").append(cn.getIpAddress()).append(":9092");
        }

        LOGGER.info("This is all advertised listeners for Kafka {}", advertisedListeners.toString());

        startZookeeper();
        startKafka();

    }

    private void startZookeeper() {
        LOGGER.info("Starting zookeeper...");
        LOGGER.info("Executing command in container with Id {}", getContainerId());

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
            .withCmd("bash", "-c", "bin/zookeeper-server-start.sh config/zookeeper.properties &")
            .exec();

        try {
            dockerClient.execStartCmd(execCreateCmdResponse.getId()).start().awaitCompletion(10, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void startKafka() {
        LOGGER.info("Starting kafka...");

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
            .withCmd("bash", "-c", "bin/kafka-server-start.sh config/server.properties --override listeners=BROKER://0.0.0.0:9092,PLAINTEXT://0.0.0.0:" + KAFKA_PORT + "  --override advertised.listeners=" + advertisedListeners.toString() + " --override zookeeper.connect=localhost:" + ZOOKEEPER_PORT + " --override listener.security.protocol.map=BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT --override inter.broker.listener.name=BROKER &")
            .exec();

        try {
            dockerClient.execStartCmd(execCreateCmdResponse.getId()).start().awaitCompletion(10, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), kafkaExposedPort);
    }
}
