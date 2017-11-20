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
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

    // TODO document lifecycle

    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    private KafkaImpl kafka;
    private AdminClient adminClient;
    private DefaultKubernetesClient kubeClient;
    private K8sImpl k8s;
    private Operator operator;

    /** Executor for processing {@link Operator.OperatorEvent}s. */
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


    /**
     * Stop the operator, waiting up to the given timeout
     * @throws InterruptedException if interrupted while waiting.
     */
    public void stop(long timeout, TimeUnit unit) throws InterruptedException {
        long initiated = System.currentTimeMillis();
        long timeoutMillis = unit.convert(timeout, TimeUnit.MILLISECONDS);
        logger.info("Stopping with timeout {}ms", timeoutMillis);
        kubeClient.close();
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
        long timeoutLeft = timeoutMillis - (System.currentTimeMillis() - initiated);
        adminClient.close(timeoutLeft, TimeUnit.MILLISECONDS);
        logger.info("Stopped");
    }

    private void start(String kafkaBootstrapServers, String kubernetesMasterUrl, String zookeeperConnect) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrapServers);
        this.adminClient = AdminClient.create(props);
        this.kafka = new KafkaImpl(adminClient, executor);

        final io.fabric8.kubernetes.client.Config config = new ConfigBuilder().withMasterUrl(kubernetesMasterUrl).build();
        this.kubeClient = new DefaultKubernetesClient(config);

        CmPredicate cmPredicate = new CmPredicate("type", "runtime",
                "kind", "topic",
                "app", "barnabas");

        this.k8s = new K8sImpl(null, kubeClient, cmPredicate);

        this.operator = new Operator(kubeClient, kafka, k8s, executor, cmPredicate);

        BootstrapWatcher bootstrap = new BootstrapWatcher(operator, zookeeperConnect);
        TopicStore topicStore = new ZkTopicStore(bootstrap);
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
                                operator.onConfigMapAdded(configMap);
                                break;
                            case MODIFIED:
                                operator.onConfigMapModified(topicStore, configMap);
                                break;
                            case DELETED:
                                operator.onConfigMapDeleted(configMap);
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


        executor.scheduleAtFixedRate(() -> {
            CompletableFuture<Set<TopicName>> kafkaTopicsFuture = kafka.listTopicsFuture();

            kafkaTopicsFuture.whenCompleteAsync((kafkaTopics, exception) -> {

                // First reconcile the topics in kafka
                for (TopicName topicName : kafkaTopics) {
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

            }, executor);
        }, 0, 15, TimeUnit.MINUTES);
    }

    public static void main(String[] args) {
        String kubernetesMasterUrl = System.getProperty("OPERATOR_K8S_URL", "https://localhost:8443");
        String kafkaBootstrapServers = System.getProperty("OPERATOR_KAFKA_HOST", "localhost") + ":" + System.getProperty("OPERATOR_KAFKA_PORT", "9092;");
        String zookeeperConnect = System.getProperty("OPERATOR_ZK_HOST", "localhost") + ":" + System.getProperty("OPERATOR_ZK_PORT", "2181;");

        Main main = new Main();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                main.stop(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("Interrupted while shutting down", e);
                // And otherwise ignore it, since we're shutting down anyway
            }
        }));
        main.start(kafkaBootstrapServers, kubernetesMasterUrl, zookeeperConnect);

    }
}
