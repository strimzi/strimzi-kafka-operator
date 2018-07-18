/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.disjoint;

public class TopicOperator {

    private final static Logger LOGGER = LogManager.getLogger(TopicOperator.class);
    private final static Logger EVENT_LOGGER = LogManager.getLogger("Event");
    private final Kafka kafka;
    private final K8s k8s;
    private final Vertx vertx;
    private final LabelPredicate cmPredicate;
    private final String namespace;
    private TopicStore topicStore;
    private final InFlight<TopicName> inFlight;
    private final Config config;

    enum EventType {
        INFO("Info"),
        WARNING("Warning");
        final String name;
        EventType(String name) {
            this.name = name;
        }
    }

    class Event implements Handler<Void> {
        private final EventType eventType;
        private final String message;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public Event(OperatorException exception, Handler<AsyncResult<Void>> handler) {
            this.involvedObject = exception.getInvolvedObject();
            this.message = exception.getMessage();
            this.handler = handler;
            this.eventType = EventType.WARNING;
        }

        public Event(HasMetadata involvedObject, String message, EventType eventType, Handler<AsyncResult<Void>> handler) {
            this.involvedObject = involvedObject;
            this.message = message;
            this.handler = handler;
            this.eventType = eventType;
        }

        @Override
        public void handle(Void v) {
            EventBuilder evtb = new EventBuilder().withApiVersion("v1");
            if (involvedObject != null) {
                evtb.withNewInvolvedObject()
                        .withKind("ConfigMap")
                        .withName(involvedObject.getMetadata().getName())
                        .withApiVersion(involvedObject.getApiVersion())
                        .withNamespace(involvedObject.getMetadata().getNamespace())
                        .withUid(involvedObject.getMetadata().getUid())
                        .endInvolvedObject();
            }
            evtb.withType(eventType.name)
                    .withMessage(message)
                    .withNewMetadata().withLabels(cmPredicate.labels()).withGenerateName("topic-operator").withNamespace(namespace).endMetadata()
                    .withNewSource()
                    .withComponent(TopicOperator.class.getName())
                    .endSource();
            io.fabric8.kubernetes.api.model.Event event = evtb.build();
            switch (eventType) {
                case INFO:
                    LOGGER.info("{}", message);
                    break;
                case WARNING:
                    LOGGER.warn("{}", message);
                    break;
            }
            k8s.createEvent(event, handler);
        }

        public String toString() {
            return "ErrorEvent(involvedObject=" + involvedObject + ", message=" + message + ")";
        }
    }

    /** Topic created in ZK */
    class CreateConfigMap implements Handler<Void> {
        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public CreateConfigMap(Topic topic, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            io.strimzi.api.kafka.model.Topic topic = TopicSerialization.toTopicResource(this.topic, cmPredicate);
            k8s.createConfigMap(topic, handler);
        }

        @Override
        public String toString() {
            return "CreateConfigMap(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** Topic deleted in ZK */
    class DeleteConfigMap implements Handler<Void> {

        private final MapName mapName;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public DeleteConfigMap(MapName mapName, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.mapName = mapName;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            k8s.deleteConfigMap(mapName, handler);
        }

        @Override
        public String toString() {
            return "DeleteConfigMap(mapName=" + mapName + ")";
        }
    }

    /** Topic config modified in ZK */
    class UpdateConfigMap implements Handler<Void> {

        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public UpdateConfigMap(Topic topic, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            io.strimzi.api.kafka.model.Topic topic = TopicSerialization.toTopicResource(this.topic, cmPredicate);
            k8s.updateConfigMap(topic, handler);
        }

        @Override
        public String toString() {
            return "UpdateConfigMap(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** ConfigMap created in k8s */
    class CreateKafkaTopic implements Handler<Void> {

        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public CreateKafkaTopic(Topic topic,
                                HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            kafka.createTopic(topic, ar -> {
                if (ar.succeeded()) {
                    LOGGER.info("Created topic '{}' for ConfigMap '{}'", topic.getTopicName(), topic.getMapName());
                    handler.handle(ar);
                } else {
                    handler.handle(ar);
                    if (ar.cause() instanceof TopicExistsException) {
                        // TODO reconcile
                    } else {
                        throw new OperatorException(involvedObject, ar.cause());
                    }
                }
            });
        }

        @Override
        public String toString() {
            return "CreateKafkaTopic(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** ConfigMap modified in k8s */
    class UpdateKafkaConfig implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;

        public UpdateKafkaConfig(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            kafka.updateTopicConfig(topic, ar -> {
                if (ar.failed()) {
                    enqueue(new Event(involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });

        }

        @Override
        public String toString() {
            return "UpdateKafkaConfig(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** ConfigMap modified in k8s */
    class IncreaseKafkaPartitions implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;

        public IncreaseKafkaPartitions(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            kafka.increasePartitions(topic, ar -> {
                if (ar.failed()) {
                    enqueue(new Event(involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });

        }

        @Override
        public String toString() {
            return "UpdateKafkaPartitions(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** ConfigMap modified in k8s */
    class ChangeReplicationFactor implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;

        public ChangeReplicationFactor(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            kafka.changeReplicationFactor(topic, ar -> {
                if (ar.failed()) {
                    enqueue(new Event(involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });

        }

        @Override
        public String toString() {
            return "ChangeReplicationFactor(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** ConfigMap deleted in k8s */
    class DeleteKafkaTopic implements Handler<Void> {

        public final TopicName topicName;
        private final Handler<AsyncResult<Void>> handler;

        public DeleteKafkaTopic(TopicName topicName, Handler<AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            LOGGER.info("Deleting topic '{}'", topicName);
            kafka.deleteTopic(topicName, handler);
        }

        @Override
        public String toString() {
            return "DeleteKafkaTopic(topicName=" + topicName + ")";
        }
    }

    public TopicOperator(Vertx vertx, Kafka kafka,
                         K8s k8s,
                         TopicStore topicStore,
                         LabelPredicate cmPredicate,
                         String namespace,
                         Config config) {
        this.kafka = kafka;
        this.k8s = k8s;
        this.vertx = vertx;
        this.cmPredicate = cmPredicate;
        this.topicStore = topicStore;
        this.inFlight = new InFlight<>(vertx);
        this.namespace = namespace;
        this.config = config;
    }

    Future<Void> reconcile(io.strimzi.api.kafka.model.Topic cm, TopicName topicName) {
        Future<Void> result = Future.future();
        Handler<Future<Void>> action = new Reconciliation("reconcile") {
            @Override
            public void handle(Future<Void> fut) {

                try {
                    Topic k8sTopic = cm != null ? TopicSerialization.fromTopicResource(cm) : null;
                    Future<Topic> topicResult = Future.future();
                    Future<TopicMetadata> metadataResult = Future.future();
                    kafka.topicMetadata(topicName, metadataResult.completer());
                    topicStore.read(topicName, topicResult.completer());
                    CompositeFuture.all(topicResult, metadataResult).setHandler(ar -> {

                        if (ar.succeeded()) {
                            Topic privateTopic = ar.result().resultAt(0);
                            TopicMetadata kafkaTopicMeta = ar.result().resultAt(1);
                            Topic kafkaTopic = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
                            reconcile(cm, k8sTopic, kafkaTopic, privateTopic, reconcileResult -> {
                                if (reconcileResult.succeeded()) {
                                    LOGGER.info("Success reconciling ConfigMap {}", logConfigMap(cm));
                                    fut.complete();
                                } else {
                                    LOGGER.error("Error reconciling ConfigMap {}", logConfigMap(cm), reconcileResult.cause());
                                    fut.fail(reconcileResult.cause());
                                }
                            });
                        } else {
                            LOGGER.error("Error reconciling ConfigMap {}", logConfigMap(cm), ar.cause());
                            fut.fail(ar.cause());
                        }
                    });
                } catch (InvalidTopicException e) {
                    LOGGER.error("Error reconciling ConfigMap {}: Invalid 'data' section: ", logConfigMap(cm), e.getMessage());
                    fut.fail(e);
                } catch (OperatorException e) {
                    LOGGER.error("Error reconciling ConfigMap {}", logConfigMap(cm), e);
                    fut.fail(e);
                }
            }
        };
        inFlight.enqueue(topicName, action, result);
        return result;
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
    void reconcile(final HasMetadata involvedObject,
                   final Topic k8sTopic, final Topic kafkaTopic, final Topic privateTopic,
                   final Handler<AsyncResult<Void>> reconciliationResultHandler) {

        {
            TopicName topicName = k8sTopic != null ? k8sTopic.getTopicName() : kafkaTopic != null ? kafkaTopic.getTopicName() : privateTopic != null ? privateTopic.getTopicName() : null;
            LOGGER.info("Reconciling topic {}, k8sTopic:{}, kafkaTopic:{}, privateTopic:{}", topicName, k8sTopic == null ? "null" : "nonnull", kafkaTopic == null ? "null" : "nonnull", privateTopic == null ? "null" : "nonnull");
        }
        if (privateTopic == null) {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // All three null: This happens reentrantly when a topic or configmap is deleted
                    LOGGER.debug("All three topics null during reconciliation.");
                    reconciliationResultHandler.handle(Future.succeededFuture());
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    LOGGER.debug("topic created in kafka, will create cm in k8s and topicStore");
                    enqueue(new CreateConfigMap(kafkaTopic, ar -> {
                        // In all cases, create in privateState
                        if (ar.succeeded()) {
                            enqueue(new CreateInTopicStore(kafkaTopic, involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar);
                        }
                    }));
                }
            } else if (kafkaTopic == null) {
                // it's been created in k8s => create in Kafka and privateState
                LOGGER.debug("cm created in k8s, will create topic in kafka and topicStore");
                enqueue(new CreateKafkaTopic(k8sTopic, involvedObject, ar -> {
                    // In all cases, create in privateState
                    if (ar.succeeded()) {
                        enqueue(new CreateInTopicStore(k8sTopic, involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            } else {
                update2Way(involvedObject, k8sTopic, kafkaTopic, reconciliationResultHandler);
            }
        } else {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // delete privateState
                    LOGGER.debug("cm deleted in k8s and topic deleted in kafka => delete from topicStore");
                    enqueue(new DeleteFromTopicStore(privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                    reconciliationResultHandler.handle(Future.succeededFuture());
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    LOGGER.debug("cm deleted in k8s => delete topic from kafka and from topicStore");
                    enqueue(new DeleteKafkaTopic(kafkaTopic.getTopicName(), ar -> {
                        if (ar.succeeded()) {
                            enqueue(new DeleteFromTopicStore(privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar);
                        }
                    }));

                }
            } else if (kafkaTopic == null) {
                // it was deleted in kafka so delete in k8s and privateState
                LOGGER.debug("topic deleted in kafkas => delete cm from k8s and from topicStore");
                enqueue(new DeleteConfigMap(privateTopic.getOrAsMapName(), ar -> {
                    if (ar.succeeded()) {
                        enqueue(new DeleteFromTopicStore(privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            } else {
                // all three exist
                LOGGER.debug("3 way diff");
                update3Way(involvedObject, k8sTopic, kafkaTopic, privateTopic, reconciliationResultHandler);
            }
        }
    }

    private void update2Way(HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Handler<AsyncResult<Void>> reconciliationResultHandler) {
        TopicDiff diff = TopicDiff.diff(kafkaTopic, k8sTopic);
        if (diff.isEmpty()) {
            // they're the same => do nothing, but stil create the private copy
            LOGGER.debug("cm created in k8s and topic created in kafka, but they're identical => just creating in topicStore");
            LOGGER.debug("k8s and kafka versions of topic '{}' are the same", kafkaTopic.getTopicName());
            enqueue(new CreateInTopicStore(kafkaTopic, involvedObject, reconciliationResultHandler));
        } else if (!diff.changesReplicationFactor()
                && !diff.changesNumPartitions()
                && diff.changesConfig()
                && disjoint(kafkaTopic.getConfig().keySet(), k8sTopic.getConfig().keySet())) {
            LOGGER.debug("cm created in k8s and topic created in kafka, they differ only in topic config, and those configs are disjoint: Updating k8s and kafka, and creating in topic store");
            Map<String, String> mergedConfigs = new HashMap<>(kafkaTopic.getConfig());
            mergedConfigs.putAll(k8sTopic.getConfig());
            Topic mergedTopic = new Topic.Builder(kafkaTopic).withConfig(mergedConfigs).build();
            enqueue(new UpdateConfigMap(mergedTopic, ar -> {
                if (ar.succeeded()) {
                    enqueue(new UpdateKafkaConfig(mergedTopic, involvedObject, ar2 -> {
                        if (ar2.succeeded()) {
                            enqueue(new CreateInTopicStore(mergedTopic, involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar2);
                        }
                    }));
                } else {
                    reconciliationResultHandler.handle(ar);
                }
            }));
        } else {
            // Just use kafka version, but also create a warning event
            LOGGER.debug("cm created in k8s and topic created in kafka, and they are irreconcilably different => kafka version wins");
            enqueue(new Event(involvedObject, "ConfigMap is incompatible with the topic metadata. " +
                    "The topic metadata will be treated as canonical.", EventType.INFO, ar -> {
                if (ar.succeeded()) {
                    enqueue(new UpdateConfigMap(kafkaTopic, ar2 -> {
                        if (ar2.succeeded()) {
                            enqueue(new CreateInTopicStore(kafkaTopic, involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar2);
                        }
                    }));
                } else {
                    reconciliationResultHandler.handle(ar);
                }
            }));
        }
    }

    private void update3Way(HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Topic privateTopic,
                            Handler<AsyncResult<Void>> reconciliationResultHandler) {
        if (!privateTopic.getMapName().equals(k8sTopic.getMapName())) {
            reconciliationResultHandler.handle(Future.failedFuture(new OperatorException(involvedObject,
                    "Topic '" + kafkaTopic.getTopicName() + "' is already managed via ConfigMap '" + privateTopic.getMapName() + "' it cannot also be managed via the ConfiMap '" + k8sTopic.getMapName() + "'")));
            return;
        }
        TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
        LOGGER.debug("topicStore->kafkaTopic: {}", oursKafka);
        TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
        LOGGER.debug("topicStore->k8sTopic: {}", oursK8s);
        String conflict = oursKafka.conflict(oursK8s);
        if (conflict != null) {
            final String message = "ConfigMap and Topic both changed in a conflicting way: " + conflict;
            LOGGER.error(message);
            enqueue(new Event(involvedObject, message, EventType.INFO, eventResult -> { }));
            reconciliationResultHandler.handle(Future.failedFuture(new Exception(message)));
        } else {
            TopicDiff merged = oursKafka.merge(oursK8s);
            LOGGER.debug("Diffs do not conflict, merged diff: {}", merged);
            if (merged.isEmpty()) {
                LOGGER.info("All three topics are identical");
                reconciliationResultHandler.handle(Future.succeededFuture());
            } else {
                Topic result = merged.apply(privateTopic);
                int partitionsDelta = merged.numPartitionsDelta();
                if (partitionsDelta < 0) {
                    final String message = "Number of partitions cannot be decreased";
                    LOGGER.error(message);
                    enqueue(new Event(involvedObject, message, EventType.INFO, eventResult -> {
                    }));
                    reconciliationResultHandler.handle(Future.failedFuture(new Exception(message)));
                } else {
                    if (merged.changesReplicationFactor()) {
                        LOGGER.error("Changes replication factor");
                        enqueue(new ChangeReplicationFactor(result, involvedObject, null));
                    }
                    // TODO What if we increase min.in.sync.replicas and the number of replicas,
                    // such that the old number of replicas < the new min isr? But likewise
                    // we could decrease, so order of tasks in the queue will need to change
                    // depending on what the diffs are.
                    LOGGER.debug("Updating cm, kafka topic and topicStore");
                    // TODO replace this with compose
                    enqueue(new UpdateConfigMap(result, ar -> {
                        Handler<Void> topicStoreHandler =
                            ignored -> enqueue(new UpdateInTopicStore(
                                result, involvedObject, reconciliationResultHandler));
                        Handler<Void> partitionsHandler;
                        if (partitionsDelta > 0) {
                            partitionsHandler = ar4 -> enqueue(new IncreaseKafkaPartitions(result, involvedObject, ar2 -> topicStoreHandler.handle(null)));
                        } else {
                            partitionsHandler = topicStoreHandler;
                        }
                        if (merged.changesConfig()) {
                            enqueue(new UpdateKafkaConfig(result, involvedObject, ar2 -> partitionsHandler.handle(null)));
                        } else {
                            enqueue(partitionsHandler);
                        }
                    }));
                }
            }
        }
    }

    void enqueue(Handler<Void> event) {
        LOGGER.debug("Enqueuing event {}", event);
        vertx.runOnContext(event);
    }

    /** Called when a topic znode is deleted in ZK */
    void onTopicDeleted(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        Handler<Future<Void>> action = new Reconciliation("onTopicDeleted") {
            @Override
            public void handle(Future<Void> fut) {
                TopicOperator.this.reconcileOnTopicChange(topicName, null, fut.completer());
            }
        };
        inFlight.enqueue(topicName, action, resultHandler);

    }

    void onTopicConfigChanged(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        Handler<Future<Void>> action = new Reconciliation("onTopicConfigChanged") {
            @Override
            public void handle(Future<Void> fut) {
                kafka.topicMetadata(topicName, metadataResult -> {
                    if (metadataResult.succeeded()) {
                        Topic topic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                        TopicOperator.this.reconcileOnTopicChange(topicName, topic, fut.completer());
                    } else {
                        fut.fail(metadataResult.cause());
                    }
                });
            }
        };
        inFlight.enqueue(topicName, action, resultHandler);
    }

    void onTopicPartitionsChanged(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        Handler<Future<Void>> action = new Reconciliation("onTopicPartitionsChanged") {
            @Override
            public void handle(Future<Void> fut) {

                // getting topic information from the private store
                topicStore.read(topicName, topicResult -> {

                    TopicMetadataHandler handler = new TopicMetadataHandler(vertx, kafka, topicName, topicMetadataBackOff()) {
                        @Override
                        public void handle(AsyncResult<TopicMetadata> metadataResult) {

                            if (metadataResult.succeeded()) {

                                // getting topic metadata from Kafka
                                Topic kafkaTopic = TopicSerialization.fromTopicMetadata(metadataResult.result());

                                // if partitions aren't changed on Kafka yet, we retry with exponential backoff
                                if (topicResult.result().getNumPartitions() == kafkaTopic.getNumPartitions()) {
                                    retry();
                                } else {
                                    LOGGER.info("Topic {} partitions changed to {}", topicName, kafkaTopic.getNumPartitions());
                                    TopicOperator.this.reconcileOnTopicChange(topicName, kafkaTopic, fut.completer());
                                }

                            } else {
                                fut.fail(metadataResult.cause());
                            }
                        }

                        @Override
                        public void onMaxAttemptsExceeded(MaxAttemptsExceededException e) {
                            // it's possible that the watched znode for partitions changes, is changed
                            // due to a reassignment if we don't observe a partition count change within the backoff
                            // no need for failing the future in this case
                            fut.complete();
                        }
                    };
                    kafka.topicMetadata(topicName, handler);
                });
            }
        };
        inFlight.enqueue(topicName, action, resultHandler);
    }

    private void reconcileOnTopicChange(TopicName topicName, Topic kafkaTopic, Handler<AsyncResult<Void>> resultHandler) {
        // TODO Here I need to lookup the name of the configmap from the name of the topic.
        // I can either do that from the topicStore, or maintain an in-memory map
        // I can then look up the CM from k8s
        topicStore.read(topicName, storeResult -> {
            if (storeResult.succeeded()) {
                Topic storeTopic = storeResult.result();
                MapName mapName = null;
                if (storeTopic != null) {
                    mapName = storeTopic.getMapName();
                } else {
                    mapName = topicName.asMapName();
                }
                k8s.getFromName(mapName, kubeResult -> {
                    if (kubeResult.succeeded()) {
                        io.strimzi.api.kafka.model.Topic topic = kubeResult.result();
                        Topic k8sTopic = TopicSerialization.fromTopicResource(topic);
                        reconcile(topic, k8sTopic, kafkaTopic, storeTopic, resultHandler);
                    } else {
                        resultHandler.handle(kubeResult.<Void>map((Void) null));
                    }
                });
            } else {
                resultHandler.handle(storeResult.<Void>map((Void) null));
            }
        });
/*
        k8s.getFromName(topicName.asMapName(), kubeResult -> {
            if (kubeResult.succeeded()) {
                ConfigMap cm = kubeResult.result();
                topicStore.read(topicName, storeResult -> {
                    if (storeResult.succeeded()) {
                        final Topic k8sTopic;
                        try {
                            k8sTopic = TopicSerialization.fromConfigMap(cm);
                        } catch (InvalidConfigMapException e) {
                            resultHandler.handle(Future.failedFuture(e));
                            return;
                        }
                        reconcile(cm, k8sTopic, kafkaTopic, storeResult.result(), resultHandler);
                    } else {
                        resultHandler.handle(storeResult.<Void>map((Void)null));
                    }
                });
            } else {
                resultHandler.handle(kubeResult.<Void>map((Void)null));
            }
        });
        */
    }

    /** Called when a topic znode is created in ZK */
    void onTopicCreated(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        // XXX currently runs on the ZK thread, requiring a synchronized inFlight
        // is it better to put this check in the topic deleted event?
        Handler<Future<Void>> action = new Reconciliation("onTopicCreated") {
            @Override
            public void handle(Future<Void> fut) {

                TopicMetadataHandler handler = new TopicMetadataHandler(vertx, kafka, topicName, topicMetadataBackOff()) {

                    @Override
                    public void handle(AsyncResult<TopicMetadata> metadataResult) {

                        if (metadataResult.succeeded()) {
                            if (metadataResult.result() == null) {
                                // In this case it is most likely that we've been notified by ZK
                                // before Kafka has finished creating the topic, so we retry
                                // with exponential backoff.
                                retry();
                            } else {
                                // We now have the metadata we need to create the
                                // ConfigMap...
                                Topic kafkaTopic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                                reconcileOnTopicChange(topicName, kafkaTopic, fut);
                            }
                        } else {
                            fut.handle(metadataResult.map((Void) null));
                        }
                    }

                    @Override
                    public void onMaxAttemptsExceeded(MaxAttemptsExceededException e) {
                        fut.fail(e);
                    }
                };
                kafka.topicMetadata(topicName, handler);
            }
        };
        inFlight.enqueue(topicName, action, resultHandler);
    }

    /** Called when a ConfigMap is added in k8s */
    void onConfigMapAdded(io.strimzi.api.kafka.model.Topic addedTopic, Handler<AsyncResult<Void>> resultHandler) {
        if (cmPredicate.test(addedTopic)) {
            final Topic k8sTopic;
            try {
                k8sTopic = TopicSerialization.fromTopicResource(addedTopic);
            } catch (InvalidTopicException e) {
                resultHandler.handle(Future.failedFuture(e));
                return;
            }
            Handler<Future<Void>> action = new Reconciliation("onConfigMapAdded") {
                @Override
                public void handle(Future<Void> fut) {
                    TopicOperator.this.reconcileOnCmChange(addedTopic, k8sTopic, false, fut);
                }
            };
            inFlight.enqueue(new TopicName(addedTopic), action, resultHandler);
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    abstract class Reconciliation implements Handler<Future<Void>> {
        private final String name;

        public Reconciliation(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name + "-" + System.identityHashCode(this);
        }
    };

    /** Called when a ConfigMap is modified in k8s */
    void onConfigMapModified(io.strimzi.api.kafka.model.Topic modifiedTopic, Handler<AsyncResult<Void>> resultHandler) {
        if (cmPredicate.test(modifiedTopic)) {
            final Topic k8sTopic;
            try {
                k8sTopic = TopicSerialization.fromTopicResource(modifiedTopic);
            } catch (InvalidTopicException e) {
                resultHandler.handle(Future.failedFuture(e));
                return;
            }
            Reconciliation action = new Reconciliation("onConfigMapModified") {
                @Override
                public void handle(Future<Void> fut) {
                    TopicOperator.this.reconcileOnCmChange(modifiedTopic, k8sTopic, true, fut);
                }
            };
            inFlight.enqueue(new TopicName(modifiedTopic), action, resultHandler);
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    private void reconcileOnCmChange(io.strimzi.api.kafka.model.Topic configMap, Topic k8sTopic, boolean isModify, Handler<AsyncResult<Void>> handler) {
        TopicName topicName = new TopicName(configMap);
        Future<TopicMetadata> f1 = Future.future();
        Future<Topic> f2 = Future.future();
        kafka.topicMetadata(topicName, f1.completer());
        topicStore.read(topicName, f2.completer());
        CompositeFuture.all(f1, f2).setHandler(ar -> {
            if (ar.succeeded()) {
                TopicMetadata topicMetadata = ar.result().resultAt(0);
                Topic kafkaTopic = TopicSerialization.fromTopicMetadata(topicMetadata);
                Topic privateTopic = ar.result().resultAt(1);
                if (privateTopic == null && isModify) {
                    enqueue(new Event(configMap, "Kafka topics cannot be renamed, but ConfigMap's data." + TopicSerialization.CM_KEY_NAME + " has changed.", EventType.WARNING, handler));
                } else {
                    reconcile(configMap, k8sTopic, kafkaTopic, privateTopic, handler);
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    /** Called when a ConfigMap is deleted in k8s */
    void onConfigMapDeleted(io.strimzi.api.kafka.model.Topic deletedTopic, Handler<AsyncResult<Void>> resultHandler) {
        if (cmPredicate.test(deletedTopic)) {
            Reconciliation action = new Reconciliation("onConfigMapDeleted") {
                @Override
                public void handle(Future<Void> fut) {
                    TopicOperator.this.reconcileOnCmChange(deletedTopic, null, false, fut);
                }
            };
            inFlight.enqueue(new TopicName(deletedTopic), action, resultHandler);
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    private class UpdateInTopicStore implements Handler<Void> {
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public UpdateInTopicStore(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            topicStore.update(topic, ar -> {
                if (ar.failed()) {
                    enqueue(new Event(involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "UpdateInTopicStore(topicName=" + topic.getTopicName() + ")";
        }
    }

    class CreateInTopicStore implements Handler<Void> {
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        private CreateInTopicStore(Topic topic, HasMetadata involvedObject,
                                   Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            LOGGER.debug("Executing {}", this);
            topicStore.create(topic, ar -> {
                LOGGER.debug("Completing {}", this);
                if (ar.failed()) {
                    LOGGER.debug("{} failed", this);
                    enqueue(new Event(involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                } else {
                    LOGGER.debug("{} succeeded", this);
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "CreateInTopicStore(topicName=" + topic.getTopicName() + ")";
        }
    }

    class DeleteFromTopicStore implements Handler<Void> {
        private final TopicName topicName;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        private DeleteFromTopicStore(TopicName topicName, HasMetadata involvedObject,
                                     Handler<AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            topicStore.delete(topicName, ar -> {
                if (ar.failed()) {
                    enqueue(new Event(involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "DeleteFromTopicStore(topicName=" + topicName + ")";
        }
    }

    public boolean isWorkInflight() {
        return inFlight.size() > 0;
    }

    /**
     * @return a new instance of BackOff with configured topic metadata max attempts
     */
    private BackOff topicMetadataBackOff() {
        return new BackOff(config.get(Config.TOPIC_METADATA_MAX_ATTEMPTS));
    }

    /**
     * @param cm ConfigMap instance to log
     * @return ConfigMap representation as namespace/name for logging purposes
     */
    static String logConfigMap(io.strimzi.api.kafka.model.Topic cm) {
        return cm != null ? cm.getMetadata().getNamespace() + "/" + cm.getMetadata().getName() : null;
    }

    Future<?> reconcileAllTopics(String reconciliationType) {
        Future topicsJoin = Future.future();
        Future mapsJoin = Future.future();
        LOGGER.info("Starting {} reconciliation", reconciliationType);
        kafka.listTopics(topicsListResult -> {
            if (topicsListResult.succeeded()) {
                Set<String> kafkaTopics = topicsListResult.result();
                LOGGER.debug("Reconciling kafka topics {}", kafkaTopics);
                // First reconcile the topics in kafka
                List<Future> topicFutures = new ArrayList<>();
                for (String name : kafkaTopics) {
                    LOGGER.debug("{} reconciliation of topic {}", reconciliationType, name);
                    TopicName topicName = new TopicName(name);
                    Future topicFuture = Future.future();
                    topicFutures.add(topicFuture);
                    k8s.getFromName(topicName.asMapName(), topicResult -> {
                        if (topicResult.succeeded()) {
                            io.strimzi.api.kafka.model.Topic topic = topicResult.result();
                            reconcile(topic, topicName).setHandler(topicFuture);
                        } else {
                            LOGGER.error("Error {} getting ConfigMap {} for topic {}",
                                    reconciliationType,
                                    topicName.asMapName(), topicName, topicResult.cause());
                            topicFuture.fail(new OperatorException("Error getting ConfigMap " + topicName.asMapName() + " during " + reconciliationType + " reconciliation", topicResult.cause()));
                        }
                    });
                }
                CompositeFuture.join(topicFutures).setHandler(topicsJoin);
                LOGGER.debug("Reconciling configmaps");
                // Then those in k8s which aren't in kafka
                k8s.listMaps(configMapsListResult -> {
                    List<Future> cmFutures = new ArrayList<>();
                    if (configMapsListResult.succeeded()) {
                        List<io.strimzi.api.kafka.model.Topic> configMaps = configMapsListResult.result();
                        Map<String, io.strimzi.api.kafka.model.Topic> configMapsMap = configMaps.stream().collect(Collectors.toMap(
                            cm -> cm.getMetadata().getName(),
                            cm -> cm));
                        configMapsMap.keySet().removeAll(kafkaTopics);
                        LOGGER.debug("Reconciling configmaps: {}", configMapsMap.keySet());
                        for (io.strimzi.api.kafka.model.Topic cm : configMapsMap.values()) {
                            LOGGER.debug("{} reconciliation of configmap {}", reconciliationType, cm.getMetadata().getName());

                            TopicName topicName = new TopicName(cm);
                            cmFutures.add(reconcile(cm, topicName));
                        }
                        CompositeFuture.join(cmFutures).setHandler(mapsJoin);
                    } else {
                        LOGGER.error("Unable to list ConfigMaps", configMapsListResult.cause());
                        mapsJoin.fail(new OperatorException("Error listing existing ConfigMaps during " + reconciliationType + " reconciliation", configMapsListResult.cause()));
                    }
                    // Finally those in private store which we've not dealt with so far...
                    // TODO ^^
                });
            } else {
                LOGGER.error("Error performing {} reconciliation", reconciliationType, topicsListResult.cause());
                OperatorException listException = new OperatorException("Error listing existing topics during " + reconciliationType + " reconciliation", topicsListResult.cause());
                topicsJoin.fail(listException);
                mapsJoin.fail(listException);
            }
        });
        return CompositeFuture.join(topicsJoin, mapsJoin);
    }
}

