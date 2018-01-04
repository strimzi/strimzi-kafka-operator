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

import com.google.common.io.Files;
import io.debezium.kafka.KafkaCluster;
import io.debezium.kafka.ZookeeperServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
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
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

@RunWith(VertxUnitRunner.class)
public class ControllerIntegrationTest {

    Logger logger = LoggerFactory.getLogger(ControllerIntegrationTest.class);

    private static final Oc OC = new Oc();

    private final LabelPredicate cmPredicate = new LabelPredicate(
            "kind", "topic",
            "app", "barnabas");

    private final Vertx vertx = Vertx.vertx();
    private KafkaCluster cluster;
    private Controller controller;
    private ControllerAssignedKafkaImpl kafka;
    private AdminClient adminClient;
    private K8sImpl k8s;
    private DefaultKubernetesClient kubeClient;
    private TopicsWatcher topicsWatcher;
    private Thread hook;

    @BeforeClass
    public static void startKube() throws Exception {
        OC.clusterUp();
    }

    @AfterClass
    public static void stopKube() throws Exception {
        OC.clusterDown();
    }

    @Before
    public void setup() throws Exception {

        cluster = new KafkaCluster();
        cluster.addBrokers(1);
        cluster.deleteDataPriorToStartup(true);
        cluster.deleteDataUponShutdown(true);
        cluster.usingDirectory(Files.createTempDir());
        cluster.startup();
        KafkaCluster c = cluster;
        hook = new Thread(() -> {
            c.shutdown();
            try {
                OC.clusterDown();
            } catch (Exception e) {}
        });
        Runtime.getRuntime().addShutdownHook(hook);

        Map<String, Object> adminClientConfig = new HashMap<>();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList());
        adminClient = AdminClient.create(adminClientConfig);

        Map<String, String> controllerConfig = new HashMap<>();
        controllerConfig.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, cluster.brokerList());
        controllerConfig.put(Config.KUBERNETES_MASTER_URL.key, OC.masterUrl());
        Config config = new Config(controllerConfig);
        kafka = new ControllerAssignedKafkaImpl(adminClient, vertx, config);

        kubeClient = new DefaultKubernetesClient(OC.masterUrl());

        k8s = new K8sImpl(vertx, kubeClient, cmPredicate);

        ZkTopicStore topicStore = new ZkTopicStore(vertx);

        ZkImpl zk = new ZkImpl(vertx, "localhost:"+ zkPort(), 2_000);
        final Handler<AsyncResult<Zk>> zkConnectHandler = ar -> {
            topicsWatcher.start(ar.result());
            //tcw.start(ar.result());
        };
        zk.disconnectionHandler(ar -> {
            // reconnect if we got disconnected
            if (ar.result() != null) {
                zk.connect(zkConnectHandler);
            }
        }).temporaryConnectionHandler(topicStore).connect(zkConnectHandler);


        // TODO The topicStore needs access to a ZooKeeper instance
        // Ideally the topicStore would use the Zk wrapper, but that's probably a bit of work
        // So maybe for now the Zk wrapper can tell the topicStore when it's connected

        controller = new Controller(vertx, kafka, k8s, topicStore, cmPredicate);

        topicsWatcher = new TopicsWatcher(controller);




    }

    private int zkPort() throws NoSuchFieldException, IllegalAccessException {
        Field zkServerField = KafkaCluster.class.getDeclaredField("zkServer");
        zkServerField.setAccessible(true);
        return ((ZookeeperServer)zkServerField.get(cluster)).getPort();
    }

    @After
    public void teardown() {
        if (topicsWatcher != null) {
            topicsWatcher.stop();
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
        if (hook != null) {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Test
    public void testTopicAdded(TestContext context) throws Exception {
        Thread.sleep(10000);// TODO fix this: wait for the topics watcher to start and discover there are no topics yet
        // Create a topic
        CreateTopicsResult crt = adminClient.createTopics(Collections.singletonList(new NewTopic("test-topic-added", 1, (short) 1)));
        crt.all().get();
        Async async = context.async();

        waitForConfigMap(context, async, "test-topic-added", cm -> cm != null, 60_000L);
    }

    private void waitForConfigMap(TestContext context, Async async, String configMapName, Predicate<ConfigMap> ready, long timeout) {
        long t0 = System.currentTimeMillis();
        vertx.setPeriodic(3_000L, timerId-> {
            // Wait for a configmap to be created
            ConfigMap cm = kubeClient.configMaps().withName(configMapName).get();
            logger.info("Polled configmap {}", configMapName);
            if (ready.test(cm)) {
                async.countDown();
                vertx.cancelTimer(timerId);
            }
            long timeLeft = timeout - (System.currentTimeMillis() - t0);
            logger.debug("Have {}ms left", timeLeft);
            if (timeLeft <= 0) {
                vertx.cancelTimer(timerId);
                context.fail("Expected the configmap to satisfy predicate by now");
            }
        });
    }
/*
    @Test
    public void testTopicDeleted(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testTopicConfigChanged(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testTopicNumPartitionsChanged(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testTopicNumReplicasChanged(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testConfigMapAdded(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testConfigMapDeleted(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testConfigMapModified(TestContext context) {
        context.fail("Implement this");
    }
    */
}
