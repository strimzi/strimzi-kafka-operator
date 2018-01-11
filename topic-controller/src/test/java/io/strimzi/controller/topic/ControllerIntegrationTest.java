/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;

import io.debezium.kafka.KafkaCluster;
import io.debezium.kafka.ZookeeperServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.controller.topic.zk.Zk;
import io.strimzi.controller.topic.zk.ZkImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

@Ignore
@RunWith(VertxUnitRunner.class)
public class ControllerIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(ControllerIntegrationTest.class);

    private static final Oc OC = new Oc();
    private static Thread ocHook = new Thread(() -> {
        try {
            OC.clusterDown();
        } catch (Exception e) {}
    });

    private final LabelPredicate cmPredicate = new LabelPredicate(
            "kind", "topic",
            "app", "barnabas");

    private final Vertx vertx = Vertx.vertx();
    private KafkaCluster cluster;
    private Controller controller;
    private volatile ControllerAssignedKafkaImpl kafka;
    private AdminClient adminClient;
    private K8sImpl k8s;
    private DefaultKubernetesClient kubeClient;
    private TopicsWatcher topicsWatcher;
    private Thread kafkaHook = new Thread() {
        public void run() {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    };
    private final long timeout = 60_000L;
    private ZkTopicStore topicStore;
    private Watch cmWatcher;
    private TopicConfigsWatcher topicsConfigWatcher;

    @BeforeClass
    public static void startKube() throws Exception {
        logger.info("Executing oc cluster up");
        // It can happen that if the VM exits abnormally the cluster remains up, and further tests don't work because
        // it appears there are two brokers with id 1, so use a shutdown hook to kill the cluster.
        Runtime.getRuntime().addShutdownHook(ocHook);
        OC.clusterUp();
    }

    @AfterClass
    public static void stopKube() throws Exception {
        OC.clusterDown();
        Runtime.getRuntime().removeShutdownHook(ocHook);
    }

    @Before
    public void setup() throws Exception {
        Runtime.getRuntime().addShutdownHook(kafkaHook);
        cluster = new KafkaCluster();
        cluster.addBrokers(1);
        cluster.deleteDataPriorToStartup(true);
        cluster.deleteDataUponShutdown(true);
        cluster.usingDirectory(Files.createTempDirectory("controller-integration-test").toFile());
        cluster.startup();

        Map<String, Object> adminClientConfig = new HashMap<>();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList());
        adminClient = AdminClient.create(adminClientConfig);

        Map<String, String> controllerConfig = new HashMap<>();
        controllerConfig.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, cluster.brokerList());
        Config config = new Config(controllerConfig);
        kafka = new ControllerAssignedKafkaImpl(adminClient, vertx, config);

        kubeClient = new DefaultKubernetesClient(OC.masterUrl());

        k8s = new K8sImpl(vertx, kubeClient, cmPredicate);

        ZkImpl zk = new ZkImpl(vertx, "localhost:"+ zkPort(), 30_000);

        topicStore = new ZkTopicStore(zk, vertx);

        controller = new Controller(vertx, kafka, k8s, topicStore, cmPredicate);

        topicsConfigWatcher = new TopicConfigsWatcher(controller);
        topicsWatcher = new TopicsWatcher(controller, topicsConfigWatcher);

        // TODO The topicStore needs access to a ZooKeeper instance
        // Ideally the topicStore would use the Zk wrapper, but that's probably a bit of work
        // So maybe for now the Zk wrapper can tell the topicStore when it's connected

    }

    private int zkPort() {
        // TODO PR for upstream debezium to get the ZK port?
        try {
            Field zkServerField = KafkaCluster.class.getDeclaredField("zkServer");
            zkServerField.setAccessible(true);
            return ((ZookeeperServer) zkServerField.get(cluster)).getPort();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void teardown() {
        if (topicsWatcher != null) {
            topicsWatcher.stop();
        }
        if (cmWatcher != null) {
            cmWatcher.close();
        }
        if (kubeClient != null) {
            kubeClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
        Runtime.getRuntime().removeShutdownHook(kafkaHook);
    }

    private void waitFor(TestContext context, Supplier<Boolean> ready, long timeout, String message) {
        Async async = context.async();
        long t0 = System.currentTimeMillis();
        vertx.setPeriodic(3_000L, timerId-> {
            // Wait for a configmap to be created
            if (ready.get()) {
                async.complete();
                vertx.cancelTimer(timerId);
            }
            long timeLeft = timeout - (System.currentTimeMillis() - t0);
            if (timeLeft <= 0) {
                vertx.cancelTimer(timerId);
                context.fail(message);
            }
        });
        async.await(timeout);
    }

    @Test
    public void testTopicAdded(TestContext context) throws Exception {
        connect(context);

        // Create a topic
        String configMapName = "test-topic-added";
        CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(configMapName, 1, (short) 1)));
        crt.all().get();

        // Wait for the configmap to be created
        waitFor(context, () -> {
            ConfigMap cm = kubeClient.configMaps().withName(configMapName).get();
            logger.info("Polled configmap {}", configMapName);
            return cm != null;
        }, timeout, "Expected the configmap to have been created by now");
    }


    @Test
    public void testTopicDeleted(TestContext context) throws Exception {
        connect(context);

        // Create a topic
        String configMapName = "test-topic-deleted";
        CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(configMapName, 1, (short) 1)));
        crt.all().get();

        // Wait for the configmap to be created
        waitFor(context, () -> {
            ConfigMap cm = kubeClient.configMaps().withName(configMapName).get();
            logger.info("Polled configmap {} waiting for creation", configMapName);
            return cm != null;
        }, timeout, "Expected the configmap to have been created by now");

        // Now we can delete the topic
        DeleteTopicsResult dlt = adminClient.deleteTopics(singletonList(configMapName));
        dlt.all().get();

        // Wait for the configmap to be deleted
        waitFor(context, () -> {
            ConfigMap cm = kubeClient.configMaps().withName(configMapName).get();
            logger.info("Polled configmap {}, waiting for deletion", configMapName);
            return cm == null;
        }, timeout, "Expected the configmap to have been deleted by now");
    }

    @Test
    public void testTopicConfigChanged(TestContext context) throws Exception {
        connect(context);
        // Create a topic
        String configMapName = "test-topic-config-changed";
        CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(configMapName, 1, (short) 1)));
        crt.all().get();

        // Wait for the configmap to be created
        waitFor(context, () -> {
            ConfigMap cm = kubeClient.configMaps().withName(configMapName).get();
            logger.info("Polled configmap {} waiting for creation", configMapName);
            return cm != null;
        }, timeout, "Expected the configmap to have been created by now");


        // Get the topic config
        ConfigResource configResource = topicConfigResource(configMapName);
        org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);

        String key = "compression.type";

        Map<String, ConfigEntry> m = new HashMap<>();
        for (ConfigEntry entry: config.entries()) {
            m.put(entry.name(), entry);
        }
        final String changedValue;
        if ("snappy".equals(m.get(key).value())) {
            changedValue = "lz4";
        } else {
            changedValue = "snappy";
        }
        m.put(key, new ConfigEntry(key, changedValue));
        logger.info("Changing topic config {} to {}", key, changedValue);

        // Update the topic config
        AlterConfigsResult cgf = adminClient.alterConfigs(singletonMap(configResource,
                new org.apache.kafka.clients.admin.Config(m.values())));
        cgf.all().get();

        // Wait for the configmap to be modified
        waitFor(context, () -> {
            ConfigMap cm = kubeClient.configMaps().withName(configMapName).get();
            logger.info("Polled configmap {}, waiting for config change", configMapName);
            String gotValue = TopicSerialization.fromConfigMap(cm).getConfig().get(key);
            logger.info("Got value {}", gotValue);
            return changedValue.equals(gotValue);
        }, timeout, "Expected the configmap to have been deleted by now");
    }

    private org.apache.kafka.clients.admin.Config getTopicConfig(ConfigResource configResource) {
        try {
            return adminClient.describeConfigs(singletonList(configResource)).values().get(configResource).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private ConfigResource topicConfigResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    @Test
    @Ignore
    public void testTopicNumPartitionsChanged(TestContext context) {
        connect(context);
        context.fail("Implement this");
    }

    @Test
    @Ignore
    public void testTopicNumReplicasChanged(TestContext context) {
        connect(context);
        context.fail("Implement this");
    }

    private ConfigMap createCm(TestContext context, String topicName) {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        ConfigMap cm = TopicSerialization.toConfigMap(topic, cmPredicate);

        // Create a CM
        kubeClient.configMaps().create(cm);

        // Wait for the topic to be created
        waitFor(context, ()-> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic to be created by now");
        return cm;
    }

    @Test
    public void testConfigMapAdded(TestContext context) {
        connect(context);

        String topicName = "test-configmap-created";
        createCm(context, topicName);
    }

    @Test
    public void testConfigMapDeleted(TestContext context) {
        connect(context);

        // create the cm
        String topicName = "test-configmap-deleted";
        ConfigMap cm = createCm(context, topicName);

        // can now delete the cm
        kubeClient.configMaps().withName(cm.getMetadata().getName()).delete();

        // Wait for the topic to be created
        waitFor(context, ()-> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return true;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic to be deleted by now");

    }

    private void connect(TestContext context) {
        waitFor(context, () -> this.topicsWatcher.started(), timeout, "Topic watcher not started");
        waitFor(context, () -> this.topicsConfigWatcher.started(), timeout, "Topic configs watcher not started");
        cmWatcher = kubeClient.configMaps().watch(new ConfigMapWatcher(controller, cmPredicate));
    }

    @Test
    public void testConfigMapModified(TestContext context) throws Exception {
        connect(context);
        // create the cm
        String topicName = "test-configmap-modified";
        ConfigMap cm = createCm(context, topicName);

        // now change the cm
        kubeClient.configMaps().withName(cm.getMetadata().getName()).edit().addToData(TopicSerialization.CM_KEY_CONFIG, "{\"retention.ms\":\"12341234\"}").done();

        // Wait for that to be reflected in the topic
        waitFor(context, ()-> {
            ConfigResource configResource = topicConfigResource(topicName);
            org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);
            String retention = config.get("retention.ms").value();
            logger.debug("retention of {}, waiting for 12341234", retention);
            return "12341234".equals(retention);
        },  timeout, "Expected the topic be be updated");
    }
}
