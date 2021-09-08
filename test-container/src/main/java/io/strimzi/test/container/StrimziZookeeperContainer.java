/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * StrimziZookeeperContainer is an instance of the Zookeeper encapsulated inside a docker container using image from
 * quay.io/strimzi/kafka with the given version. It can be combined with @StrimziKafkaContainer but we suggest to use
 * directly @StrimziKafkaCluster for more complicated testing.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class StrimziZookeeperContainer extends GenericContainer<StrimziZookeeperContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziZookeeperContainer.class);

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final List<String> SUPPORTED_KAFKA_VERSIONS = new ArrayList<>(5);

    public static final int ZOOKEEPER_PORT = 2181;

    private int zookeeperExposedPort;

    public StrimziZookeeperContainer(final String imageVersion) {
        super("quay.io/strimzi/kafka:" + imageVersion);
        super.withNetwork(Network.SHARED);

        // exposing zookeeper port from the container
        withExposedPorts(ZOOKEEPER_PORT);
        withNetworkAliases("zookeeper");

        withEnv("LOG_DIR", "/tmp");
        withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT));
        // env for readiness
        withEnv("ZOO_4LW_COMMANDS_WHITELIST", "rouk");
    }

    public StrimziZookeeperContainer(Map<String, String> additionalKafkaConfiguration) {
        this(StrimziKafkaContainer.getStrimziVersion() + "-kafka-" + StrimziKafkaContainer.getLatestKafkaVersion());
    }

    public StrimziZookeeperContainer() {
        this(StrimziKafkaContainer.getStrimziVersion() + "-kafka-" + StrimziKafkaContainer.getLatestKafkaVersion());
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

        zookeeperExposedPort = getMappedPort(ZOOKEEPER_PORT);

        LOGGER.info("This is mapped port {}", zookeeperExposedPort);

        String command = "#!/bin/bash \n";

        command += "bin/zookeeper-server-start.sh config/zookeeper.properties\n";

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }
}
