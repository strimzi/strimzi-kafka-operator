package io.enmasse.barnabas.operator.topic;/*
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


public class Operator {

    private final static Logger logger = LoggerFactory.getLogger(Operator.class);
    private final Kafka kafka;
    private final K8s k8s;
    private final TopicStore topicStore;

    private KubernetesClient client;
    private AdminClient adminClient;
    private InFlight inFlight = new InFlight();

    /** Executor for processing {@link OperatorEvent}s. */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setUncaughtExceptionHandler((thread, exception) -> {
                if (exception instanceof OperatorException) {
                    enqueue(new ErrorEvent((OperatorException)exception));
                } else {
                    logger.error("Uncaught exception when processing events", exception);
                }
            });
            t.setName("topic-operator-executor");
            return t;
        }
    });


    static abstract class OperatorEvent implements Runnable {

        public abstract void process() throws OperatorException;

        public void run() {
            logger.info("Processing event {}", this);
            this.process();
            logger.info("Event {} processed successfully", this);
        }

        public abstract String toString();
    }

    public class ErrorEvent extends OperatorEvent {

        private final String message;
        private final HasMetadata involvedObject;

        public ErrorEvent(OperatorException exception) {
            this.involvedObject = exception.getInvolvedObject();
            this.message = exception.getMessage();
        }

        public ErrorEvent(HasMetadata involvedObject, String message) {
            this.involvedObject = involvedObject;
            this.message = message;
        }

        @Override
        public void process() {
            String myHost = "";
            EventBuilder evtb = new EventBuilder().withApiVersion("v1");
            if (involvedObject != null) {
                evtb.withNewInvolvedObject()
                        .withKind(involvedObject.getKind())
                        .withName(involvedObject.getMetadata().getName())
                        .withApiVersion(involvedObject.getApiVersion())
                        .withNamespace(involvedObject.getMetadata().getNamespace())
                        .withUid(involvedObject.getMetadata().getUid())
                        .endInvolvedObject();
            }
            evtb.withType("Warning")
                    .withMessage(this.getClass().getSimpleName() + " failed: " + message)
                    //.withReason("")
                    .withNewSource()
                    .withHost(myHost)
                    .withComponent(Operator.class.getName())
                    .endSource();
            Event event = evtb.build();
            k8s.createEvent(event);
        }

        public String toString() {
            return "ErrorEvent(involvedObject="+involvedObject+", message="+message+")";
        }
    }


    /** Topic created in ZK */
    public class CreateConfigMap extends OperatorEvent {
        private final Topic topic;

        public CreateConfigMap(Topic topic) {
            this.topic = topic;
        }

        @Override
        public void process() throws OperatorException {
            ConfigMap cm = TopicSerialization.toConfigMap(topic);
            k8s.createConfigMap(cm);
        }

        @Override
        public String toString() {
            return "CreateConfigMap(topicName="+topic.getTopicName()+")";
        }
    }

    /** Topic deleted in ZK */
    public class DeleteConfigMap extends OperatorEvent {

        private final TopicName topicName;

        public DeleteConfigMap(TopicName topicName) {
            this.topicName = topicName;
        }

        @Override
        public void process() {
            k8s.deleteConfigMap(topicName);
        }

        @Override
        public String toString() {
            return "DeleteConfigMap(topicName="+topicName+")";
        }
    }

    /** Topic config modified in ZK */
    public class UpdateConfigMap extends OperatorEvent {

        private final Topic topic;
        private final HasMetadata involvedObject;

        public UpdateConfigMap(Topic topic, HasMetadata involvedObject) {
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() {
            // TODO get topic data from AdminClient
            // How do we avoid a race here, where the topic exists in ZK, but not yet visible from AC?
            // Record that it's us who is creating to the config map
            // create ConfigMap in k8s
            // ignore the watch for the configmap creation
            ConfigMap cm = TopicSerialization.toConfigMap(topic);
            k8s.updateConfigMap(cm);
        }

        @Override
        public String toString() {
            return "UpdateConfigMap(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap created in k8s */
    public class CreateKafkaTopic extends OperatorEvent {

        private final Topic topic;

        private final HasMetadata involvedObject;

        public CreateKafkaTopic(Topic topic, HasMetadata involvedObject) {
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            kafka.createTopic(TopicSerialization.toNewTopic(topic), (ar) -> {
                if (ar.isSuccess()) {
                    logger.info("Created topic '{}' for ConfigMap '{}'", topic.getTopicName(), topic.getMapName());
                } else {
                    if (ar.exception() instanceof TopicExistsException) {
                        // TODO reconcile
                    } else {
                        throw new OperatorException(involvedObject, ar.exception());
                    }
                }
            });
        }

        @Override
        public String toString() {
            return "CreateKafkaTopic(topicName="+ topic.getTopicName()+")";
        }
    }

    /** ConfigMap modified in k8s */
    public class UpdateKafkaConfig extends OperatorEvent {

        private final HasMetadata involvedObject;

        private final Topic topic;

        public UpdateKafkaConfig(Topic topic, HasMetadata involvedObject) {
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            kafka.updateTopicConfig(topic, ar-> {
                if (!ar.isSuccess()) {
                    enqueue(new ErrorEvent(involvedObject, ar.exception().toString()));
                }
            });

        }

        @Override
        public String toString() {
            return "UpdateKafkaConfig(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap modified in k8s */
    public class UpdateKafkaPartitions extends OperatorEvent {

        private final HasMetadata involvedObject;

        private final Topic topic;

        public UpdateKafkaPartitions(Topic topic, HasMetadata involvedObject) {
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            kafka.increasePartitions(topic, ar-> {
                if (!ar.isSuccess()) {
                    enqueue(new ErrorEvent(involvedObject, ar.exception().toString()));
                }
            });

        }

        @Override
        public String toString() {
            return "UpdateKafkaPartitions(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap deleted in k8s */
    public class DeleteKafkaTopic extends OperatorEvent {

        public final TopicName topicName;

        private final HasMetadata involvedObject;

        public DeleteKafkaTopic(TopicName topicName, HasMetadata involvedObject) {
            this.topicName = topicName;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            logger.info("Deleting topic '{}'", topicName);
            kafka.deleteTopic(topicName, (result) -> {
                if (result.isSuccess()) {
                    logger.info("Deleted topic '{}' for ConfigMap", topicName);
                } else {
                    throw new OperatorException(involvedObject, result.exception());
                }
            });

            // TODO we need to do better than simply logging on error
            // -- can we produce some kind of error event in k8s?

            // -- really we want an error to propagate out of the Kubernetes API for deleting the config map
            //    but that's only really an option with a CRD and customer operator
        }

        @Override
        public String toString() {
            return "DeleteKafkaTopic(topicName="+topicName+")";
        }
    }




    public Operator(String kubernetesMasterUrl, String kafkaBootstrapServers, String zookeeperConnect) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrapServers);
        adminClient = AdminClient.create(props);
        this.kafka = new KafkaImpl(adminClient, executor, inFlight);

        final io.fabric8.kubernetes.client.Config config = new ConfigBuilder().withMasterUrl(kubernetesMasterUrl).build();
        client = new DefaultKubernetesClient(config);
        this.k8s = new K8sImpl(client, inFlight);

        this.topicStore = new ZkTopicStore(null);

        logger.info("Connecting to ZooKeeper");
        new BootstrapWatcher(this, zookeeperConnect);

        Thread configMapThread = new Thread(() -> {
            client.configMaps().watch(new Watcher<ConfigMap>() {
                public void eventReceived(Action action, ConfigMap configMap) {
                    ObjectMeta metadata = configMap.getMetadata();
                    Map<String, String> labels = metadata.getLabels();

                    String name = metadata.getName();
                    logger.info("ConfigMap watch received event {} on map {} with labels {}", action, name, labels);
                    logger.info("ConfigMap {} was created {}", name, metadata.getCreationTimestamp());
                    if ("barnabas".equals(labels.get("app"))
                            && "runtime".equals(labels.get("type"))
                            && "topic".equals(labels.get("kind"))) {
                        switch (action) {
                            case ADDED:
                                onConfigMapAdded(configMap);
                                break;
                            case MODIFIED:
                                onConfigMapModified(configMap);
                                break;
                            case DELETED:
                                onConfigMapDeleted(configMap);
                                break;
                            case ERROR:
                                logger.error("Watch received action=ERROR for ConfigMap " + name);
                        }
                    }
                }

                public void onClose(KubernetesClientException e) {
                    // TODO well I guess we need to reconnect
                }
            });
        });
        configMapThread.setName("cm-watcher");
        logger.debug("Starting {}", configMapThread);
        configMapThread.start();

        executor.scheduleAtFixedRate(() -> {
            CompletableFuture<Set<TopicName>> kafkaTopicsFuture = kafka.listTopicsFuture();

            kafkaTopicsFuture.whenCompleteAsync((kafkaTopics, exception) -> {

                // First reconcile the topics in kafka
                for (TopicName topicName : kafkaTopics) {
                    // TODO need to check inflight
                    // Reconciliation
                    ConfigMap cm = k8s.getFromName(topicName.asMapName());
                    reconcile(cm, topicName);
                }

                // Then those in k8s which aren't in kafka
                List<ConfigMap> configMaps = k8s.listMaps();
                Map<String, ConfigMap> configMapsMap = configMaps.stream().collect(Collectors.toMap(
                        cm -> cm.getMetadata().getName(),
                        cm -> cm));
                configMapsMap.keySet().removeAll(kafkaTopics);
                for (ConfigMap cm : configMapsMap.values()) {
                    TopicName topicName = new TopicName(cm);
                    reconcile(cm, topicName);
                }

                // Finally those in private store which we've not dealt with so far...
                // TODO ^^
            }, executor);
        }, 0, 15, TimeUnit.MINUTES);
    }

    private void reconcile(ConfigMap cm, TopicName topicName) {
        Topic k8sTopic = TopicSerialization.fromConfigMap(cm);
        CompletableFuture<TopicMetadata> kafkaTopicMeta = kafka.topicMetadata(topicName, 0, TimeUnit.MILLISECONDS);
        CompletableFuture<Topic> privateState = topicStore.read(topicName);
        kafkaTopicMeta.runAfterBothAsync(privateState, ()-> {
            Topic kafkaTopic = null;
            try {
                kafkaTopic = TopicSerialization.fromTopicMetadata(kafkaTopicMeta.get());
                Topic privateTopic = privateState.get();
                reconcile(cm, k8sTopic, kafkaTopic, privateTopic);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }, executor);
    }

    /**
     * 0. Set up some persistent ZK nodes for us
     * 1. When updating CM, we also update our ZK nodes
     * 2. When updating Kafka, we also update our ZK nodes
     * 3. When reconciling we get all three versions of the Topic, k8s, kafka and privateState
     *   - If privateState doesn't exist:
     *     - If k8s doesn't exist, we reason it's been created in kafka and we create it k8s from kafka
     *     - If kafka doesn't exist, we reason it's been created in k8s, and we create it in kafka from k8s
     *     - If both exist, and are the same: That's fine
     *     - If both exist, and are different: We use whichever has the most recent mtime.
     *     - In all above cases we create privateState
     *   - If privateState does exist:
     *     - If k8s doesn't exist, we reason it was deleted, and delete kafka
     *     - If kafka doesn't exist, we reason it was delete and we delete k8s
     *     - If neither exists, we delete privateState.
     *     - If both exist then all three exist, and we need to reconcile:
     *       - We compute diff privateState->k8s and privateState->kafka and we merge the two
     *         - If there are conflicts => error
     *         - Otherwise we apply the apply the merged diff to privateState, and use that for both k8s and kafka
     *     - In all above cases we update privateState
     * Topic identification should be by uid/cxid, not by name.
     * Topic identification should be by uid/cxid, not by name.
     */
    private void reconcile(HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Topic privateTopic) {
        if (privateTopic == null) {
            final Topic source;
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // All three null? This shouldn't be possible
                    logger.error("All three topics null during reconciliation. This should be impossible.");
                    return;
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    enqueue(new CreateConfigMap(kafkaTopic));
                    source = kafkaTopic;
                }
            } else if (kafkaTopic == null) {
                // it's been created in k8s => create in Kafka and privateState
                enqueue(new CreateKafkaTopic(k8sTopic, involvedObject));
                source = k8sTopic;
            } else if (TopicDiff.diff(kafkaTopic, k8sTopic).isEmpty()) {
                // they're the same => do nothing
                logger.debug("k8s and kafka versions of topic '{}' are the same", kafkaTopic.getTopicName());
                source = kafkaTopic;
            } else {
                // TODO use whichever has the most recent mtime
                throw new RuntimeException("Not implemented");
            }

            // In all cases, create in privateState
            enqueue(new CreateInTopicStore(source, involvedObject));
        } else {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // delete privateState
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    enqueue(new DeleteKafkaTopic(k8sTopic.getTopicName(), involvedObject));
                }
            } else if (kafkaTopic == null) {
                // it was deleted in kafka so delete in k8s and privateState
                enqueue(new DeleteConfigMap(k8sTopic.getTopicName()));
            } else {
                // all three exist
                TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
                TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
                String conflict = oursKafka.conflict(oursK8s);
                if (conflict != null) {
                    enqueue(new ErrorEvent(involvedObject, "ConfigMap and Topic both changed in a conflicting way: " + conflict));
                } else {
                    TopicDiff merged = oursKafka.merge(oursK8s);
                    Topic result = merged.apply(privateTopic);
                    if (merged.changesReplicationFactor()) {
                        enqueue(new ErrorEvent(involvedObject, "Topic Replication Factor cannot be changed"));
                    } else {
                        enqueue(new UpdateConfigMap(result, involvedObject));
                        if (merged.changesConfig()) {
                            enqueue(new UpdateKafkaConfig(result, involvedObject));
                        }
                        if (merged.changesNumPartitions()) {
                            enqueue(new UpdateKafkaPartitions(result, involvedObject));
                        }
                        enqueue(new UpdateTopicStore(result, involvedObject));
                    }
                }
            }
        }
    }

    void enqueue(OperatorEvent event) {
        logger.info("Enqueuing event {}", event);
        executor.execute(event);
    }

    /** Called when a topic znode is deleted in ZK */
    void onTopicDeleted(TopicName topicName) {
        // XXX currently runs on the ZK thread, requiring a synchronized `pending`
        // is it better to put this check in the topic deleted event?
        // that would require exposing an API to remove()
        if (inFlight.shouldProcessDelete(topicName)) {
            enqueue(new DeleteConfigMap(topicName));
        }
    }

    /** Called when a topic znode is created in ZK */
    void onTopicCreated(TopicName topicName) {
        // XXX currently runs on the ZK thread, requiring a synchronized pending
        // is it better to put this check in the topic deleted event?
        if (inFlight.shouldProcessTopicCreate(topicName)) {
            BiConsumer<TopicMetadata, Throwable> handler = new BiConsumer<TopicMetadata, Throwable>() {
                BackOff backOff = new BackOff();

                @Override
                public void accept(TopicMetadata metadata, Throwable throwable) {
                    if (throwable != null) {
                        kafka.topicMetadata(topicName, backOff.delayMs(), TimeUnit.MILLISECONDS).whenComplete(this);
                    } else {
                        Topic topic = TopicSerialization.fromTopicMetadata(metadata);
                        enqueue(new CreateConfigMap(topic));
                    }
                }
            };
            kafka.topicMetadata(topicName, 0, TimeUnit.MILLISECONDS).whenComplete(handler);
        }
    }

    /** Called when a ConfigMap is added in k8s */
    private void onConfigMapAdded(ConfigMap configMap) {
        TopicName topicName = new TopicName(configMap);
        if (inFlight.shouldProcessConfigMapAdded(topicName)) {
            Topic topic = TopicSerialization.fromConfigMap(configMap);
            enqueue(new CreateKafkaTopic(topic, configMap));
        }
    }

    /** Called when a ConfigMap is modified in k8s */
    private void onConfigMapModified(ConfigMap configMap) {
        TopicName topicName = new TopicName(configMap);
        if (inFlight.shouldProcessConfigMapModified(topicName)) {
            // We don't know what's changed in the ConfigMap
            // it could be #partitions and/or config and/or replication factor
            // So call reconcile, rather than enqueuing a UpdateKafkaTopic directly
            reconcile(configMap, topicName);
            //enqueue(new UpdateKafkaTopic(topic, configMap));
        }
    }

    /** Called when a ConfigMap is deleted in k8s */
    private void onConfigMapDeleted(ConfigMap configMap) {
        TopicName topicName = new TopicName(configMap);
        if (inFlight.shouldProcessConfigMapDeleted(topicName)) {
            enqueue(new DeleteKafkaTopic(topicName, configMap));
        }
    }

    /**
     * Stop the operator, waiting up to the given timeout
     * @throws InterruptedException if interrupted while waiting.
     */
    public void stop(long timeout, TimeUnit unit) throws InterruptedException {
        long initiated = System.currentTimeMillis();
        long timeoutMillis = unit.convert(timeout, TimeUnit.MILLISECONDS);
        logger.info("Stopping with timeout {}ms", timeoutMillis);
        client.close();
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
        long timeoutLeft = timeoutMillis - (System.currentTimeMillis() - initiated);
        adminClient.close(timeoutLeft, TimeUnit.MILLISECONDS);
        logger.info("Stopped");
    }

    private class UpdateTopicStore extends OperatorEvent {
        private final Topic topic;
        private final HasMetadata involvedObject;

        public UpdateTopicStore(Topic topic, HasMetadata involvedObject) {
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            topicStore.update(topic).whenCompleteAsync((vr, throwable) -> {
                if (throwable != null) {
                    enqueue(new ErrorEvent(involvedObject, throwable.toString()));
                }
            }, executor);
        }

        @Override
        public String toString() {
            return "UpdateTopicStore(topicName="+topic.getTopicName()+")";
        }
    }

    private class CreateInTopicStore extends OperatorEvent {
        private final Topic topic;
        private final HasMetadata involvedObject;

        public CreateInTopicStore(Topic topic, HasMetadata involvedObject) {
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            topicStore.update(topic).whenCompleteAsync((vr, throwable) -> {
                if (throwable != null) {
                    enqueue(new ErrorEvent(involvedObject, throwable.toString()));
                }
            }, executor);
        }

        @Override
        public String toString() {
            return "CreateInTopicStore(topicName="+topic.getTopicName()+")";
        }
    }
}

