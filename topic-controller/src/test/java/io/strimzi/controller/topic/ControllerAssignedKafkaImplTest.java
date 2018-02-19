/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

@RunWith(VertxUnitRunner.class)
public class ControllerAssignedKafkaImplTest {

    private static Config config;

    static {

        Map<String, String> map = new HashMap<>();
        map.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        map.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        map.put(Config.NAMESPACE.key, "default");
        map.put(Config.REASSIGN_VERIFY_INTERVAL_MS.key, "1 seconds");
        config = new Config(map);
    }

    /**
     * We subclass the class under test so that we can run our own executable
     * rather than the real ReassignPartitionsCommand. This allows us to test
     * various scenarios without needing ZK+Kafka clusters deployed.
     */
    static class Subclass extends ControllerAssignedKafkaImpl {

        static List<String> fail(String message) {
            return asList("--fail", message);
        }

        static List<String> generate(String current, String propose) {
            return asList("--generate-current", current, "--generate-propose", propose);
        }

        static List<String> executeStarted() {
            return asList("--execute-started");
        }

        static List<String> verifyInProgress(String... partitions) {
            List<String> result = new ArrayList(2 * partitions.length);
            for (String partition: partitions) {
                result.add("--verify-in-progress");
                result.add(partition);
            }
            return result;
        }

        static List<String> verifySuccess(String... partitions) {
            List<String> result = new ArrayList(2 * partitions.length);
            for (String partition: partitions) {
                result.add("--verify-success");
                result.add(partition);
            }
            return result;
        }

        static List<String> verifyFail(String message) {
            return asList("--verify-fail", message);
        }

        static List<String> executeFail(String message) {
            return asList("--execute-fail", message);
        }

        static List<String> executeInProgress() {
            return asList("--execute-running");
        }

        public static final String SCRIPT = "src/test/scripts/reassign.sh";
        private final List<List<String>> args;
        private final String script;
        private int i = 0;

        public Subclass(AdminClient adminClient, Vertx vertx, Config config, List<List<String>> args) {
            this(adminClient, vertx, config, SCRIPT, args);
        }

        public Subclass(AdminClient adminClient, Vertx vertx, Config config, String script, List<List<String>> args) {
            super(adminClient, vertx, config);
            this.script = script;
            this.args = args;
        }

        @Override
        protected void addJavaArgs(List<String> args) {
            args.add(script);
            args.addAll(this.args.get(i++));
        }
    }

    @Test
    public void changeReplicationFactor_missingExecutable(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        final String doesNotExist = "/some/executable/that/does/not/exist";
        Subclass sub = new Subclass(adminClient, vertx, config, doesNotExist, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            context.assertFalse(ar.succeeded());
            final String message = ar.cause().getMessage();
            context.assertTrue(message.contains("lacks an executable arg[0]")
                    && message.contains("/some/executable/that/does/not/exist"));
            async.complete();
        });
    }

    @Test
    public void changeReplicationFactor_notExecutable(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, "pom.xml", asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            context.assertFalse(ar.succeeded());
            final String message = ar.cause().getMessage();
            context.assertTrue(message.contains("lacks an executable arg[0]")
                && message.contains("pom.xml"));
            async.complete();
        });
    }

    @Test
    public void changeReplicationFactor(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            context.assertTrue(ar.succeeded());
            async.complete();
        });
    }

    /**
     * Test the case where an error happens during --verify execution.
     * We should retry until we succeed (or (TODO) a timeout happens).
     * We handle this differently from case of error in --execute because we need to ensure throttles get removed.
     */
    @Test
    public void changeReplicationFactor_TransientErrorInVerify(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.fail("Bang!"),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            // We should retry anf ultimately succeed
            context.assertTrue(ar.succeeded());
            async.complete();
        });
    }

    @Test
    public void changeReplicationFactor_ErrorInVerify(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifyFail("Bang!"),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            // We should retry anf ultimately succeed
            context.assertTrue(ar.succeeded());
            async.complete();
        });
    }

    /**
     * Test the case where the --gexecute execution fails because a reassignment is currently running.
     * We should give up and fail the handler, on the basis that we will retry later as a result of
     * periodic reconciliation.
     */
    @Test
    public void changeReplicationFactor_ExecuteInProgress(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeInProgress(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            context.assertFalse(ar.succeeded());
            context.assertEquals("Reassigment failed: There is an existing assignment running.", ar.cause().getMessage());
            async.complete();
        });
    }

    /**
     * Test the case where something goes wrong with the initial --generate execution.
     * We should give up and fail the handler, on the basis that we will retry later as a result of
     * periodic reconciliation.
     */
    @Test
    public void changeReplicationFactor_ExecuteFail(TestContext context) {
        MockAdminClient adminClient = new MockAdminClient();
        Vertx vertx = Vertx.vertx();
        Topic topic = new Topic.Builder("changeReplicationFactor", 2, (short) 2, emptyMap()).build();
        String[] partitions = new String[]{"changeReplicationFactor-0", "changeReplicationFactor-1"};
        Subclass sub = new Subclass(adminClient, vertx, config, asList(
                Subclass.generate("{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}",
                        "{\"version\":1,\"partitions\":[{\"topic\":\"test-topic\",\"partition\":0,\"replicas\":[0],\"log_dirs\":[\"any\"]},{\"topic\":\"test-topic\",\"partition\":1,\"replicas\":[0],\"log_dirs\":[\"any\"]}]}"),
                Subclass.executeFail("Bang!"),
                Subclass.executeStarted(),
                Subclass.verifyInProgress(partitions),
                Subclass.verifySuccess(partitions)));
        Async async = context.async();
        sub.changeReplicationFactor(topic, ar -> {
            context.assertFalse(ar.succeeded());
            context.assertTrue(ar.cause().getMessage().contains("Failed to reassign partitions"));
            async.complete();
        });
    }

}
