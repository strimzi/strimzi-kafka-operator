/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * An implementation of {@link Kafka} which leave partition assignment decisions to the Kafka controller.
 * The controller is able to make rack-aware assignments (if so configured), but does not take into account
 * other aspects (e.g. disk utilisation, CPU load, network IO).
 */
public class ControllerAssignedKafkaImpl extends BaseKafkaImpl {

    private final static Logger LOGGER = LoggerFactory.getLogger(ControllerAssignedKafkaImpl.class);
    private final Config config;

    public ControllerAssignedKafkaImpl(AdminClient adminClient, Vertx vertx, Config config) {
        super(adminClient, vertx);
        this.config = config;
    }

    @Override
    public void increasePartitions(Topic topic, Handler<AsyncResult<Void>> handler) {
        final NewPartitions newPartitions = NewPartitions.increaseTo(topic.getNumPartitions());
        final Map<String, NewPartitions> request = Collections.singletonMap(topic.getTopicName().toString(), newPartitions);
        KafkaFuture<Void> future = adminClient.createPartitions(request).values().get(topic.getTopicName().toString());
        queueWork(new UniWork<>("increasePartitions", future, handler));
    }

    /**
     * Create a new topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    @Override
    public void createTopic(Topic topic, Handler<AsyncResult<Void>> handler) {
        NewTopic newTopic = TopicSerialization.toNewTopic(topic, null);

        LOGGER.debug("Creating topic {}", newTopic);
        KafkaFuture<Void> future = adminClient.createTopics(
                Collections.singleton(newTopic)).values().get(newTopic.name());
        queueWork(new UniWork<>("createTopic", future, handler));
    }

    @Override
    public void changeReplicationFactor(Topic topic, Handler<AsyncResult<Void>> handler) {

        LOGGER.info("Changing replication factor of topic {} to {}", topic.getTopicName(), topic.getNumReplicas());

        final String zookeeper = config.get(Config.ZOOKEEPER_CONNECT);
        Future<File> generateFuture = Future.future();

        // generate a reassignment
        vertx.executeBlocking(fut -> {
            try {
                LOGGER.debug("Generating reassignment json for topic {}", topic.getTopicName());
                String reassignment = generateReassignment(topic, zookeeper);
                LOGGER.debug("Reassignment json for topic {}: {}", topic.getTopicName(), reassignment);
                File reassignmentJsonFile = createTmpFile("-reassignment.json");
                try (Writer w = new OutputStreamWriter(new FileOutputStream(reassignmentJsonFile), StandardCharsets.UTF_8)) {
                    w.write(reassignment);
                }
                fut.complete(reassignmentJsonFile);
            } catch (Exception e) {
                fut.fail(e);
            }
        },
            generateFuture.completer());

        Future<File> executeFuture = Future.future();

        generateFuture.compose(reassignmentJsonFile -> {
            // execute the reassignment
            vertx.executeBlocking(fut -> {
                final Long throttle = config.get(Config.REASSIGN_THROTTLE);
                try {
                    LOGGER.debug("Starting reassignment for topic {} with throttle {}", topic.getTopicName(), throttle);
                    executeReassignment(reassignmentJsonFile, zookeeper, throttle);
                    fut.complete(reassignmentJsonFile);
                } catch (Exception e) {
                    fut.fail(e);
                }
            },
                executeFuture.completer());
        }, executeFuture);

        Future<Void> periodicFuture = Future.future();
        Future<Void> reassignmentFinishedFuture = Future.future();

        executeFuture.compose(reassignmentJsonFile -> {
            // Poll repeatedly, calling --verify to remove the throttle
            long timeout = 10_000;
            long first = System.currentTimeMillis();
            final Long periodMs = config.get(Config.REASSIGN_VERIFY_INTERVAL_MS);
            LOGGER.debug("Verifying reassignment every {} seconds", TimeUnit.SECONDS.convert(periodMs, TimeUnit.MILLISECONDS));
            vertx.setPeriodic(periodMs, timerId ->
                vertx.<Boolean>executeBlocking(fut -> {
                    LOGGER.debug(String.format("Verifying reassignment for topic {} (timer id=%s)", topic.getTopicName(), timerId));

                    final Long throttle = config.get(Config.REASSIGN_THROTTLE);
                    final boolean reassignmentComplete;
                    try {
                        reassignmentComplete = verifyReassignment(reassignmentJsonFile, zookeeper, throttle);
                    } catch (Exception e) {
                        fut.fail(e);
                        return;
                    }
                    fut.complete(reassignmentComplete);
                },
                    ar -> {
                        if (ar.succeeded()) {
                            if (ar.result()) {
                                LOGGER.info("Reassignment complete");
                                delete(reassignmentJsonFile);
                                LOGGER.debug("Cancelling timer " + timerId);
                                vertx.cancelTimer(timerId);
                                reassignmentFinishedFuture.complete();
                            } else if (System.currentTimeMillis() - first > timeout) {
                                LOGGER.error("Reassignment timed out");
                                delete(reassignmentJsonFile);
                                LOGGER.debug("Cancelling timer " + timerId);
                                vertx.cancelTimer(timerId);
                                reassignmentFinishedFuture.fail("Timeout");
                            }
                        } else {
                            //reassignmentFinishedFuture.fail(ar.cause());
                            LOGGER.error("Error while verifying reassignment", ar.cause());
                        }
                    }
                )
            );
            periodicFuture.complete();
        },
            periodicFuture);


        CompositeFuture.all(periodicFuture, reassignmentFinishedFuture).map((Void) null).setHandler(handler);

        // TODO The algorithm should really be more like this:
        // 1. Use the cmdline tool to generate an assignment
        // 2. Set the throttles
        // 3. Update the reassign_partitions znode
        // 4. Watch for changes or deletion of reassign_partitions
        //    a. Update the throttles
        //    b. complete the handler
        // Doing this is much better because means we don't have to batch reassignments
        // and also means we need less state for reassignment
        // though we aren't relieved of the statefullness wrt removing throttles :-(
    }

    private static void delete(File file) {
        /*if (!file.delete()) {
            logger.warn("Unable to delete temporary file {}", file);
        }*/
    }

    private static File createTmpFile(String suffix) throws IOException {
        File tmpFile = File.createTempFile(ControllerAssignedKafkaImpl.class.getName(), suffix);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Created temporary file {}", tmpFile);
        }
        /*tmpFile.deleteOnExit();*/
        return tmpFile;
    }

    private static class VerifyLineParser implements Function<String, Void> {
        int complete = 0;
        int inProgress = 0;

        @Override
        public Void apply(String line) {
            if (line.contains("Partitions reassignment failed due to")
                    || Pattern.matches("Reassignment of partition .* failed", line)) {
                throw new ControllerException("Reassigment failed: " + line);
            } else if (Pattern.matches("Reassignment of partition .* completed successfully", line)) {
                complete++;
            } else if (Pattern.matches("Reassignment of partition .* is still in progress", line)) {
                inProgress++;
            }
            return null;
        }
    }

    private boolean verifyReassignment(File reassignmentJsonFile, String zookeeper, Long throttle) throws IOException, InterruptedException {
        List<String> verifyArgs = new ArrayList<>();
        addJavaArgs(verifyArgs);
        // command args
        verifyArgs.add("--zookeeper");
        verifyArgs.add(zookeeper);
        if (throttle != null) {
            verifyArgs.add("--throttle");
            verifyArgs.add(Long.toString(throttle));
        }
        verifyArgs.add("--reassignment-json-file");
        verifyArgs.add(reassignmentJsonFile.toString());
        verifyArgs.add("--verify");
        VerifyLineParser verifyLineParser = new VerifyLineParser();
        executeSubprocess(verifyArgs).forEachLineStdout(verifyLineParser);
        return verifyLineParser.inProgress == 0;
    }

    private void executeReassignment(File reassignmentJsonFile, String zookeeper, Long throttle) throws IOException, InterruptedException {
        List<String> executeArgs = new ArrayList<>();
        addJavaArgs(executeArgs);
        executeArgs.add("--zookeeper");
        executeArgs.add(zookeeper);
        if (throttle != null) {
            executeArgs.add("--throttle");
            executeArgs.add(Long.toString(throttle));
        }
        executeArgs.add("--reassignment-json-file");
        executeArgs.add(reassignmentJsonFile.toString());
        executeArgs.add("--execute");

        if (!executeSubprocess(executeArgs).forEachLineStdout(line -> {
            if (line.contains("Partitions reassignment failed due to")
                    || line.contains("There is an existing assignment running")
                    || line.contains("Failed to reassign partitions")) {
                throw new TransientControllerException("Reassigment failed: " + line);
            } else if (line.contains("Successfully started reassignment of partitions.")) {
                return true;
            } else {
                return null;
            }
        })) {
            throw new TransientControllerException("Reassignment execution neither failed nor finished");
        }
    }

    private String generateReassignment(Topic topic, String zookeeper) throws IOException, InterruptedException, ExecutionException {
        JsonFactory factory = new JsonFactory();

        File topicsToMove = createTmpFile("-topics-to-move.json");

        try (JsonGenerator gen = factory.createGenerator(topicsToMove, JsonEncoding.UTF8)) {
            gen.writeStartObject();
            gen.writeNumberField("version", 1);
            gen.writeArrayFieldStart("topics");
            gen.writeStartObject();
            gen.writeStringField("topic", topic.getTopicName().toString());
            gen.writeEndObject();
            gen.writeEndArray();
            gen.writeEndObject();
            gen.flush();
        }
        List<String> executeArgs = new ArrayList<>();
        addJavaArgs(executeArgs);
        executeArgs.add("--zookeeper");
        executeArgs.add(zookeeper);
        executeArgs.add("--topics-to-move-json-file");
        executeArgs.add(topicsToMove.toString());
        executeArgs.add("--broker-list");
        executeArgs.add(brokerList());
        executeArgs.add("--generate");

        final ProcessResult processResult = executeSubprocess(executeArgs);
        delete(topicsToMove);
        String json = processResult.forEachLineStdout(new ReassignmentLineParser());
        return json;

    }

    /** Use the AdminClient to get a comma-separated list of the broker ids in the Kafka cluster */
    private String brokerList() throws InterruptedException, ExecutionException {
        StringBuilder sb = new StringBuilder();
        for (Node node: adminClient.describeCluster().nodes().get()) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(node.id());
        }
        return sb.toString();
    }

    protected void addJavaArgs(List<String> verifyArgs) {
        // protected access only for testing purposes

        // use the same java executable that's executing this code
        verifyArgs.add(System.getProperty("java.home") + "/bin/java");
        // use the same classpath as we have
        verifyArgs.add("-cp");
        verifyArgs.add(System.getProperty("java.class.path"));
        // main
        verifyArgs.add("kafka.admin.ReassignPartitionsCommand");
    }

    private ProcessResult executeSubprocess(List<String> verifyArgs) throws IOException, InterruptedException {
        // We choose to run the reassignment as an external process because the Scala class:
        //  a) doesn't throw on errors, but
        //  b) writes them to stdout
        // so we need to parse its output, but we can't do that in an isolated way if we run it in our process
        // (System.setOut being global to the VM).

        if (verifyArgs.isEmpty() || !new File(verifyArgs.get(0)).canExecute()) {
            throw new ControllerException("Command " + verifyArgs + " lacks an executable arg[0]");
        }

        ProcessBuilder pb = new ProcessBuilder(verifyArgs);
        // If we redirect stderr to stdout we could break the predicates because the
        // characters will be jumbled.
        // Reading two pipes without deadlocking on the blocking is difficult, so let's just write stderr to a file.
        File stdout = createTmpFile(".out");
        File stderr = createTmpFile(".err");
        pb.redirectError(stderr);
        pb.redirectOutput(stdout);
        Process p = pb.start();
        LOGGER.info("Started process {} with command line {}", p, verifyArgs);
        p.getOutputStream().close();
        int exitCode = p.waitFor();
        // TODO timeout on wait
        LOGGER.info("Process {}: exited with status {}", p, exitCode);
        return new ProcessResult(p, stdout, stderr);
    }



    private static class ProcessResult implements AutoCloseable {
        private final File stdout;
        private final File stderr;
        private final Object pid;

        ProcessResult(Object pid, File stdout, File stderr) {
            this.pid = pid;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public <T> T forEachLineStdout(Function<String, T> fn) throws IOException {
            return forEachLine(this.stdout, fn);
        }

        private <T> T forEachLine(File file, Function<String, T> fn) throws IOException {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    // Use platform default charset, on assumption that
                    // the ReassignPartitionsCommand will output in that
                    new FileInputStream(file), Charset.defaultCharset()))) {
                String line = reader.readLine();
                while (line != null) {
                    LOGGER.debug("Process {}: stdout: {}", pid, line);
                    T result = fn.apply(line);
                    if (result != null) {
                        return result;
                    }
                    line = reader.readLine();
                }
                return null;
            }
        }

        @Override
        public void close() throws Exception {
            delete(stdout);
            delete(stderr);
        }
    }

    private static class ReassignmentLineParser implements Function<String, String> {
        boolean returnLine = false;

        @Override
        public String apply(String line) {
            if (line.contains("Partitions reassignment failed due to")) {
                throw new TransientControllerException("Reassignment failed: " + line);
            }
            if (returnLine) {
                return line;
            }
            if (line.contains("Proposed partition reassignment configuration")) {
                // Return the line following this one, since that's the JSON representation of the reassignment
                returnLine = true;
            }
            return null;
        }
    }
}

