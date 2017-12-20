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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main extends AbstractVerticle {

    // TODO document lifecycle

    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    public static final String OPERATOR_CM_NAME = "topic-operator";

    private Config config;
    private ControllerAssignedKafkaImpl kafka;
    private AdminClient adminClient;
    private DefaultKubernetesClient kubeClient;
    private K8sImpl k8s;
    private Operator operator;

    public Main(@NotNull Config config) {
        this.config = config;
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

        final io.fabric8.kubernetes.client.Config kubeConfig = new ConfigBuilder().withMasterUrl(this.config.get(Config.KUBERNETES_MASTER_URL)).build();
        this.kubeClient = new DefaultKubernetesClient(kubeConfig);

        // TODO watch a configmap for this operator and redeploy the verticle on changes to that CM

        Properties adminClientProps = new Properties();
        adminClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        this.adminClient = AdminClient.create(adminClientProps);
        this.kafka = new ControllerAssignedKafkaImpl(adminClient, vertx, config);

        // app=barnabas and kind=topic
        // or app=barnabas, kind=topic, cluster=my-cluster if we need to scope it to a cluster
        LabelPredicate cmPredicate = new LabelPredicate("kind", "topic",
                "app", "barnabas");

        this.k8s = new K8sImpl(null, kubeClient, cmPredicate);

        this.operator = new Operator(vertx, kafka, k8s, cmPredicate);

        ZkTopicStore topicStore = new ZkTopicStore(vertx);

        TopicsWatcher tw = new TopicsWatcher(operator);
        TopicConfigsWatcher tcw = new TopicConfigsWatcher(operator);
        Zk zk = Zk.create(vertx, config.get(Config.ZOOKEEPER_CONNECT), this.config.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());
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

        vertx.setPeriodic(this.config.get(Config.FULL_RECONCILIATION_INTERVAL_MS),
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
        HashMap<String, String> c = new HashMap<>();
        c.put(Config.KUBERNETES_MASTER_URL.key, System.getProperty("OPERATOR_K8S_URL", "https://localhost:8443"));
        c.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, System.getProperty("OPERATOR_KAFKA_HOST", "localhost") + ":" + System.getProperty("OPERATOR_KAFKA_PORT", "9092"));
        c.put(Config.ZOOKEEPER_CONNECT.key, System.getProperty("OPERATOR_ZK_HOST", "localhost") + ":" + System.getProperty("OPERATOR_ZK_PORT", "2181;"));
        Main main = new Main(new Config(c));
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(main);

    }
}
