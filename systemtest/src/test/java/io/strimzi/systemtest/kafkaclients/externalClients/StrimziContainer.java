package io.strimzi.systemtest.kafkaclients.externalClients;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class StrimziContainer extends GenericContainer<StrimziContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziContainer.class);

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final int KAFKA_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;

    private int kafkaExposedPort;

    public StrimziContainer(final String version) {
        super("strimzi/kafka:" + version);
        super.withNetwork(Network.SHARED);

        // exposing kafka port from the container
        withExposedPorts(KAFKA_PORT);

        withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181");
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

        startZookeeper();
        startKafka();

        kafkaExposedPort = getMappedPort(KAFKA_PORT);
        LOGGER.info("This is mapped port {}", kafkaExposedPort);


        // TODO: here we need to append env variable KAFKA_ADVERTISED_LISTENERS to be able visible from the container...
//        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
//            .withPrivileged(true)
//            .withAttachStdin(true)
//            .withAttachStdout(true)
//            .withAttachStderr(true)
//            .withCmd("bash", "-c", "export KAFKA_ADVERTISED_LISTENERS='BROKER://localhost:" + kafkaExposedPort)
//            .exec();
//
//        OutputStream outputStream = new ByteArrayOutputStream();
//
//        try {
//            dockerClient.execStartCmd(execCreateCmdResponse.getId())
//                .withDetach(false)
//                .withTty(true)
//                .exec(new ExecStartResultCallback(outputStream, System.err))
//                .awaitCompletion();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    private void startZookeeper() {
        LOGGER.info("Starting zookeeper...");
        LOGGER.info("Executing command in container with Id {}", getContainerId());

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
            .withPrivileged(true)
            .withAttachStdin(true)
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd("bash", "-c", "bin/zookeeper-server-start.sh config/zookeeper.properties &")
            .exec();

        try {
            OutputStream outputStream = new ByteArrayOutputStream();
            String output;

            dockerClient.execStartCmd(execCreateCmdResponse.getId())
                .withDetach(false)
                .withTty(true)
                .exec(new ExecStartResultCallback(outputStream, System.err))
                .awaitCompletion();

            output = outputStream.toString();
            LOGGER.debug(output);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.info("localhost:" + ZOOKEEPER_PORT);
    }

    private void startKafka() {
        LOGGER.info("Starting kafka...");

        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(getContainerId())
            .withPrivileged(true)
            .withAttachStdin(true)
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd("bash", "-c", "bin/kafka-server-start.sh config/server.properties &")
            .exec();

        try {
            OutputStream outputStream = new ByteArrayOutputStream();
            String output;

            dockerClient.execStartCmd(execCreateCmdResponse.getId())
                .withDetach(false)
                .withTty(true)
                .exec(new ExecStartResultCallback(outputStream, System.err))
                .awaitCompletion();

            output = outputStream.toString();
            LOGGER.debug(output);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.info("localhost:" + KAFKA_PORT);
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), kafkaExposedPort);
    }
}
