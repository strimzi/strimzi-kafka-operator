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

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.topic.zk.Zk;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Session extends AbstractVerticle {

    private final static Logger logger = LoggerFactory.getLogger(Session.class);

    private final Config config;
    private final KubernetesClient kubeClient;

    private ControllerAssignedKafkaImpl kafka;
    private AdminClient adminClient;
    private K8sImpl k8s;
    private Controller controller;
    private Watch topicCmWatch;
    private TopicsWatcher tw;
    private TopicConfigsWatcher tcw;
    private volatile boolean stopped = false;

    public Session(KubernetesClient kubeClient, Config config) {
        this.kubeClient = kubeClient;
        this.config = config;
    }

    /**
     * Stop the controller.
     */
    public void stop() {
        this.stopped = true;
        logger.info("Stopping");
        logger.debug("Stopping kube watch");
        topicCmWatch.close();
        logger.debug("Stopping zk watches");
        tw.stop();
        tcw.stop();
        // TODO wait for inflight to "empty"
        adminClient.close(1, TimeUnit.MINUTES);
        logger.info("Stopped");
    }

    @Override
    public void start() {
        Properties adminClientProps = new Properties();
        adminClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        this.adminClient = AdminClient.create(adminClientProps);
        this.kafka = new ControllerAssignedKafkaImpl(adminClient, vertx, config);

        // app=barnabas and kind=topic
        // or app=barnabas, kind=topic, cluster=my-cluster if we need to scope it to a cluster
        LabelPredicate cmPredicate = new LabelPredicate("kind", "topic",
                "app", "strimzi");

        this.k8s = new K8sImpl(vertx, kubeClient, cmPredicate);

        Zk zk = Zk.create(vertx, config.get(Config.ZOOKEEPER_CONNECT), this.config.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());

        ZkTopicStore topicStore = new ZkTopicStore(zk, vertx);

        this.controller = new Controller(vertx, kafka, k8s, topicStore, cmPredicate);

        this.tcw = new TopicConfigsWatcher(controller);
        this.tw = new TopicsWatcher(controller, tcw);
        tw.start(zk);
        tcw.start(zk);

        Thread configMapThread = new Thread(() -> {
            logger.debug("Watching configmaps matching {}", cmPredicate);
            Session.this.topicCmWatch = kubeClient.configMaps().inNamespace(kubeClient.getNamespace()).watch(new ConfigMapWatcher(controller, cmPredicate));
            logger.debug("Watching setup");
        }, "configmap-watcher");
        logger.debug("Starting {}", configMapThread);
        configMapThread.start();

//        // Reconcile initially
//        reconcileTopics("initial");
//        // And periodically after that
//        vertx.setPeriodic(this.config.get(Config.FULL_RECONCILIATION_INTERVAL_MS),
//                (timerId) -> {
//                    if (stopped) {
//                        vertx.cancelTimer(timerId);
//                        return;
//                    }
//                    reconcileTopics("periodic");
//                });
    }

    private void reconcileTopics(String reconciliationType) {
        logger.info("Starting {} reconciliation", reconciliationType);
        kafka.listTopics(arx -> {
            if (arx.succeeded()) {
                Set<String> kafkaTopics = arx.result();
                logger.debug("Reconciling kafka topics {}", kafkaTopics);
                // First reconcile the topics in kafka
                for (String name : kafkaTopics) {
                    logger.debug("{} reconciliation of topic {}", reconciliationType, name);
                    TopicName topicName = new TopicName(name);
                    k8s.getFromName(topicName.asMapName(), ar -> {
                        ConfigMap cm = ar.result();
                        // TODO need to check inflight
                        // TODO And need to prevent pileup of inflight periodic reconciliations
                        controller.reconcile(cm, topicName);
                    });
                }

                logger.debug("Reconciling configmaps");
                // Then those in k8s which aren't in kafka
                k8s.listMaps(ar -> {
                    List<ConfigMap> configMaps = ar.result();
                    Map<String, ConfigMap> configMapsMap = configMaps.stream().collect(Collectors.toMap(
                            cm -> cm.getMetadata().getName(),
                            cm -> cm));
                    configMapsMap.keySet().removeAll(kafkaTopics);
                    logger.debug("Reconciling configmaps: {}", configMapsMap.keySet());
                    for (ConfigMap cm : configMapsMap.values()) {
                        logger.debug("{} reconciliation of configmap {}", reconciliationType, cm.getMetadata().getName());
                        // TODO need to check inflight
                        // TODO And need to prevent pileup of inflight periodic reconciliations
                        TopicName topicName = new TopicName(cm);
                        controller.reconcile(cm, topicName);
                    }

                    // Finally those in private store which we've not dealt with so far...
                    // TODO ^^
                });
            } else {
                logger.error("Error performing {} reconciliation", reconciliationType, arx.cause());
            }
        });
    }

}
