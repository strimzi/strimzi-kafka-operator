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

package io.enmasse.barnabas.operator.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main extends AbstractVerticle {

    // TODO document lifecycle

    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    private final String kafkaBootstrapServers;
    private final String kubernetesMasterUrl;
    private final String zookeeperConnect;
    private KafkaImpl kafka;
    private AdminClient adminClient;
    private DefaultKubernetesClient kubeClient;
    private K8sImpl k8s;
    private Operator operator;

    /** Executor for processing {@link Operator.OperatorEvent}s. */
    /*
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setUncaughtExceptionHandler((thread, exception) -> {
                if (exception instanceof OperatorException) {
                    operator.enqueue(operator.new ErrorEvent((OperatorException)exception));
                } else {
                    logger.error("Uncaught exception when processing events", exception);
                }
            });
            t.setName("topic-operator-executor");
            return t;
        }
    });
    */


    public Main(String kafkaBootstrapServers, String kubernetesMasterUrl, String zookeeperConnect) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kubernetesMasterUrl = kubernetesMasterUrl;
        this.zookeeperConnect = zookeeperConnect;
    }

    /**
     * Stop the operator.
     */
    public void stop() {
        // We should:
        // 1. Stop passing on notifications from ZK and K8s
        // 2. Wait for in-flight work to cease
        logger.info("Stopping");
        kubeClient.close();
        adminClient.close(1, TimeUnit.MINUTES);
        logger.info("Stopped");
    }

    @Override
    public void start() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrapServers);
        this.adminClient = AdminClient.create(props);
        this.kafka = new KafkaImpl(adminClient, vertx);

        final io.fabric8.kubernetes.client.Config config = new ConfigBuilder().withMasterUrl(kubernetesMasterUrl).build();
        this.kubeClient = new DefaultKubernetesClient(config);

        CmPredicate cmPredicate = new CmPredicate("type", "runtime",
                "kind", "topic",
                "app", "barnabas");

        this.k8s = new K8sImpl(null, kubeClient, cmPredicate);

        this.operator = new Operator(vertx, kafka, k8s, cmPredicate);

        ZkTopicStore topicStore = new ZkTopicStore(vertx);

        TopicsWatcher tw = new TopicsWatcher(operator);
        TopicConfigsWatcher tcw = new TopicConfigsWatcher(operator);
        Zk zk = Zk.create(vertx, zookeeperConnect, 60_000);
        final Handler<AsyncResult<Zk>> zkConnectHandler = ar -> {
            tw.start(ar.result());
            tcw.start(ar.result());
        };
        zk.disconnectionHandler(ar -> {
            // reconnect if we got disconnected
            if (ar.result() != null) {
                zk.connect(zkConnectHandler);
            }
        }).connect(zkConnectHandler);

        Thread configMapThread = new Thread(() -> {
            kubeClient.configMaps().watch(new Watcher<ConfigMap>() {
                public void eventReceived(Action action, ConfigMap configMap) {
                    ObjectMeta metadata = configMap.getMetadata();
                    Map<String, String> labels = metadata.getLabels();

                    String name = metadata.getName();
                    logger.info("ConfigMap watch received event {} on map {} with labels {}", action, name, labels);
                    logger.info("ConfigMap {} was created {}", name, metadata.getCreationTimestamp());
                    if (cmPredicate.test(configMap)) {
                        switch (action) {
                            case ADDED:
                                operator.onConfigMapAdded(configMap, ar -> {});
                                break;
                            case MODIFIED:
                                operator.onConfigMapModified(configMap, ar -> {});
                                break;
                            case DELETED:
                                operator.onConfigMapDeleted(configMap, ar -> {});
                                break;
                            case ERROR:
                                logger.error("Watch received action=ERROR for ConfigMap " + name);
                        }
                    }
                }

                public void onClose(KubernetesClientException e) {
                    // TODO reconnect, unless shutting down
                }
            });
        }, "configmap-watcher");
        logger.debug("Starting {}", configMapThread);
        configMapThread.start();


        vertx.setPeriodic(TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES),
                (timerId) -> {

            kafka.listTopics(arx -> {
                if (arx.succeeded()) {
                    Set<String> kafkaTopics = arx.result();
                    // First reconcile the topics in kafka
                    for (String name : kafkaTopics) {
                        TopicName topicName = new TopicName(name);
                        // TODO need to check inflight
                        // Reconciliation
                        k8s.getFromName(topicName.asMapName(), ar -> {
                            ConfigMap cm = ar.result();
                            operator.reconcile(cm, topicName);
                        });
                    }

                    // Then those in k8s which aren't in kafka
                    k8s.listMaps(ar -> {
                        List<ConfigMap> configMaps = ar.result();
                        Map<String, ConfigMap> configMapsMap = configMaps.stream().collect(Collectors.toMap(
                                cm -> cm.getMetadata().getName(),
                                cm -> cm));
                        configMapsMap.keySet().removeAll(kafkaTopics);
                        for (ConfigMap cm : configMapsMap.values()) {
                            TopicName topicName = new TopicName(cm);
                            operator.reconcile(cm, topicName);
                        }

                        // Finally those in private store which we've not dealt with so far...
                        // TODO ^^
                    });
                }
            });
        });
    }

    public static void main(String[] args) {
        String kubernetesMasterUrl = System.getProperty("OPERATOR_K8S_URL", "https://localhost:8443");
        String kafkaBootstrapServers = System.getProperty("OPERATOR_KAFKA_HOST", "localhost") + ":" + System.getProperty("OPERATOR_KAFKA_PORT", "9092;");
        String zookeeperConnect = System.getProperty("OPERATOR_ZK_HOST", "localhost") + ":" + System.getProperty("OPERATOR_ZK_PORT", "2181;");

        Main main = new Main(kafkaBootstrapServers, kubernetesMasterUrl, zookeeperConnect);

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(main);

        /*
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                main.stop(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("Interrupted while shutting down", e);
                // And otherwise ignore it, since we're shutting down anyway
            }
        }));
        main.start(kafkaBootstrapServers, kubernetesMasterUrl, zookeeperConnect);
        */
    }
}
