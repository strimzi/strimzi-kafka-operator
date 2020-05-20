/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StrimziKafkaContainer extends GenericContainer<StrimziKafkaContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainer.class);

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final int KAFKA_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String LATEST_KAFKA_VERSION;
    private static final long STARTUP_COMPONENT_TIMEOUT_MS = Duration.ofMinutes(10).toMillis();


    private int kafkaExposedPort;
    private StringBuilder advertisedListeners;
    private static List<String> supportedKafkaVersions = new ArrayList<>(3);

    static {
        try (
            InputStream inputStream = StrimziKafkaContainer.class.getResourceAsStream("/kafka-versions.txt");
            InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(streamReader)) {

            String kafkaVersion;

            while ((kafkaVersion = bufferedReader.readLine()) != null) {
                supportedKafkaVersions.add(kafkaVersion);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER.info("This is all supported Kafka versions {}", supportedKafkaVersions.toString());

        // sort kafka version from low to high
        Collections.sort(supportedKafkaVersions);

        LATEST_KAFKA_VERSION = supportedKafkaVersions.get(supportedKafkaVersions.size() - 1);
    }

    public StrimziKafkaContainer(final String version) {
        super("strimzi/kafka:" + version);
        super.withNetwork(Network.SHARED);

        // exposing kafka port from the container
        withExposedPorts(KAFKA_PORT);
    }

    public StrimziKafkaContainer() {
        this("latest-kafka-" + LATEST_KAFKA_VERSION);
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
            advertisedListeners.append("," + "BROKER://").append(cn.getIpAddress()).append(":9093");
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
            dockerClient.execStartCmd(execCreateCmdResponse.getId()).start().awaitCompletion(STARTUP_COMPONENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void startKafka() {
        LOGGER.info("Starting kafka...");

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
            .withAttachStdout(true)
            .withAttachStdin(true)
            .withAttachStderr(true)
            .withTty(false)
            .withCmd("bash", "-c", "bin/kafka-server-start.sh config/server.properties --override listeners=BROKER://0.0.0.0:9093,PLAINTEXT://0.0.0.0:" + KAFKA_PORT + "  --override advertised.listeners=" + advertisedListeners.toString() + " --override zookeeper.connect=localhost:" + ZOOKEEPER_PORT + " --override listener.security.protocol.map=BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT --override inter.broker.listener.name=BROKER &")
            .exec();

        try (OutputStream outputStream = new ByteArrayOutputStream();
             OutputStream errorStream = new ByteArrayOutputStream()) {

            dockerClient.execStartCmd(execCreateCmdResponse.getId()).withDetach(false).exec(new ExecStartResultCallback(outputStream, errorStream)).awaitCompletion();

            LOGGER.info("OUTPUT STREAM");
            LOGGER.info(outputStream);
            LOGGER.info("=================");
            LOGGER.info("ERROR STREAM");
            LOGGER.info(errorStream);

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), kafkaExposedPort);
    }

    public static List<String> getSupportedKafkaVersions() {
        return supportedKafkaVersions;
    }

    public static String getLatestKafkaVersion() {
        return LATEST_KAFKA_VERSION;
    }
}
