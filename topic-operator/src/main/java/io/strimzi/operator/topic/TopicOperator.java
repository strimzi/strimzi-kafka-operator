/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
    private final Labels labels;
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
                        .withKind(involvedObject.getKind())
                        .withName(involvedObject.getMetadata().getName())
                        .withApiVersion(involvedObject.getApiVersion())
                        .withNamespace(involvedObject.getMetadata().getNamespace())
                        .withUid(involvedObject.getMetadata().getUid())
                        .endInvolvedObject();
            }
            evtb.withType(eventType.name)
                    .withMessage(message)
                    .withNewMetadata().withLabels(labels.labels()).withGenerateName("topic-operator").withNamespace(namespace).endMetadata()
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
    class CreateResource implements Handler<Void> {
        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public CreateResource(Topic topic, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(this.topic, labels);
            k8s.createResource(kafkaTopic, handler);
        }

        @Override
        public String toString() {
            return "CreateResource(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** Topic deleted in ZK */
    class DeleteResource implements Handler<Void> {

        private final ResourceName resourceName;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public DeleteResource(ResourceName resourceName, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.resourceName = resourceName;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            k8s.deleteResource(resourceName, handler);
        }

        @Override
        public String toString() {
            return "DeleteResource(mapName=" + resourceName + ")";
        }
    }

    /** Topic config modified in ZK */
    class UpdateResource implements Handler<Void> {

        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public UpdateResource(Topic topic, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(this.topic, labels);
            k8s.updateResource(kafkaTopic, handler);
        }

        @Override
        public String toString() {
            return "UpdateResource(topicName=" + topic.getTopicName() + ")";
        }
    }

    /** Resource created in k8s */
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
                    LOGGER.info("Created topic '{}' for KafkaTopic '{}'", topic.getTopicName(), topic.getResourceName());
                    handler.handle(ar);
                } else {
                    handler.handle(ar);
                    if (ar.cause() instanceof TopicExistsException) {
                        // TODO reconcile
                    } else if (ar.cause() instanceof InvalidReplicationFactorException) {
                        // error message is printed in the `reconcile` method
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

    /** KafkaTopic modified in k8s */
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

    /** KafkaTopic modified in k8s */
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

    /** KafkaTopic modified in k8s */
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

    /** KafkaTopic deleted in k8s */
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
                         Labels labels,
                         String namespace,
                         Config config) {
        this.kafka = kafka;
        this.k8s = k8s;
        this.vertx = vertx;
        this.labels = labels;
        this.topicStore = topicStore;
        this.inFlight = new InFlight<>(vertx);
        this.namespace = namespace;
        this.config = config;
    }

    /**
     * 0. Set up some persistent ZK nodes for us
     * 1. When updating KafkaTopic, we also update our ZK nodes
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
                    // All three null: This happens reentrantly when a topic or KafkaTopic is deleted
                    LOGGER.debug("All three topics null during reconciliation.");
                    reconciliationResultHandler.handle(Future.succeededFuture());
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    LOGGER.debug("topic created in kafka, will create KafkaTopic in k8s and topicStore");
                    enqueue(new CreateResource(kafkaTopic, ar -> {
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
                LOGGER.debug("KafkaTopic created in k8s, will create topic in kafka and topicStore");
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
                    LOGGER.debug("KafkaTopic deleted in k8s and topic deleted in kafka => delete from topicStore");
                    enqueue(new DeleteFromTopicStore(privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    LOGGER.debug("KafkaTopic deleted in k8s => delete topic from kafka and from topicStore");
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
                LOGGER.debug("topic deleted in kafkas => delete KafkaTopic from k8s and from topicStore");
                enqueue(new DeleteResource(privateTopic.getOrAsKubeName(), ar -> {
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
            LOGGER.debug("KafkaTopic created in k8s and topic created in kafka, but they're identical => just creating in topicStore");
            LOGGER.debug("k8s and kafka versions of topic '{}' are the same", kafkaTopic.getTopicName());
            enqueue(new CreateInTopicStore(kafkaTopic, involvedObject, reconciliationResultHandler));
        } else if (!diff.changesReplicationFactor()
                && !diff.changesNumPartitions()
                && diff.changesConfig()
                && disjoint(kafkaTopic.getConfig().keySet(), k8sTopic.getConfig().keySet())) {
            LOGGER.debug("KafkaTopic created in k8s and topic created in kafka, they differ only in topic config, and those configs are disjoint: Updating k8s and kafka, and creating in topic store");
            Map<String, String> mergedConfigs = new HashMap<>(kafkaTopic.getConfig());
            mergedConfigs.putAll(k8sTopic.getConfig());
            Topic mergedTopic = new Topic.Builder(kafkaTopic).withConfig(mergedConfigs).build();
            enqueue(new UpdateResource(mergedTopic, ar -> {
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
            LOGGER.debug("KafkaTopic created in k8s and topic created in kafka, and they are irreconcilably different => kafka version wins");
            enqueue(new Event(involvedObject, "KafkaTopic is incompatible with the topic metadata. " +
                    "The topic metadata will be treated as canonical.", EventType.INFO, ar -> {
                if (ar.succeeded()) {
                    enqueue(new UpdateResource(kafkaTopic, ar2 -> {
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
        if (!privateTopic.getResourceName().equals(k8sTopic.getResourceName())) {
            reconciliationResultHandler.handle(Future.failedFuture(new OperatorException(involvedObject,
                    "Topic '" + kafkaTopic.getTopicName() + "' is already managed via KafkaTopic '" + privateTopic.getResourceName() + "' it cannot also be managed via the KafkaTopic '" + k8sTopic.getResourceName() + "'")));
            return;
        }
        TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
        LOGGER.debug("topicStore->kafkaTopic: {}", oursKafka);
        TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
        LOGGER.debug("topicStore->k8sTopic: {}", oursK8s);
        String conflict = oursKafka.conflict(oursK8s);
        if (conflict != null) {
            final String message = "KafkaTopic resource and Kafka topic both changed in a conflicting way: " + conflict;
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
                        enqueue(new ChangeReplicationFactor(result, involvedObject, res -> LOGGER.error(
                                "Changing replication factor is not supported")));
                    }
                    // TODO What if we increase min.in.sync.replicas and the number of replicas,
                    // such that the old number of replicas < the new min isr? But likewise
                    // we could decrease, so order of tasks in the queue will need to change
                    // depending on what the diffs are.
                    LOGGER.debug("Updating KafkaTopic, kafka topic and topicStore");
                    // TODO replace this with compose
                    TopicDiff diff = TopicDiff.diff(k8sTopic, result);
                    if (diff.isEmpty()) {
                        LOGGER.debug("No need to update KafkaTopic with {}", diff);
                        enqueue(updateTopicStoreAndKafka(involvedObject, reconciliationResultHandler, merged, result, partitionsDelta));
                    } else {
                        LOGGER.debug("Updating KafkaTopic with {}", diff);
                        UpdateResource event = new UpdateResource(result, ar -> {
                            enqueue(updateTopicStoreAndKafka(involvedObject, reconciliationResultHandler, merged, result, partitionsDelta));
                        });
                        enqueue(event);
                    }
                }
            }
        }
    }

    private Handler<Void> updateTopicStoreAndKafka(HasMetadata involvedObject, Handler<AsyncResult<Void>> reconciliationResultHandler, TopicDiff merged, Topic topic, int partitionsDelta) {
        Handler<Void> topicStoreHandler =
            ignored -> enqueue(new UpdateInTopicStore(topic, involvedObject, reconciliationResultHandler));
        Handler<Void> partitionsHandler;
        if (partitionsDelta > 0) {
            partitionsHandler = ar4 -> enqueue(new IncreaseKafkaPartitions(topic, involvedObject, ar2 -> topicStoreHandler.handle(null)));
        } else {
            partitionsHandler = topicStoreHandler;
        }

        Handler<Void> result;
        if (merged.changesConfig()) {
            result = new UpdateKafkaConfig(topic, involvedObject, ar2 -> partitionsHandler.handle(null));
        } else {
            result = partitionsHandler;
        }
        return result;
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

    /**
     * Called when ZK watch notifies of change to topic's config
     */
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

    /**
     * Called when ZK watch notifies of a change to the topic's partitions
     */
    void onTopicPartitionsChanged(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        Handler<Future<Void>> action = new Reconciliation("onTopicPartitionsChanged") {
            @Override
            public void handle(Future<Void> fut) {

                // getting topic information from the private store
                topicStore.read(topicName, topicResult -> {

                    TopicMetadataHandler handler = new TopicMetadataHandler(vertx, kafka, topicName, topicMetadataBackOff()) {
                        @Override
                        public void handle(AsyncResult<TopicMetadata> metadataResult) {
                            try {
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
                            } catch (Throwable t) {
                                fut.fail(t);
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

    /**
     * Called when one of the ZK watches notifies of a change to the topic
     */
    private void reconcileOnTopicChange(TopicName topicName, Topic kafkaTopic, Handler<AsyncResult<Void>> resultHandler) {
        // TODO Here I need to lookup the name of the kafkatopic from the name of the topic.
        // I can either do that from the topicStore, or maintain an in-memory map
        // I can then look up the KafkaTopic from k8s
        topicStore.read(topicName, storeResult -> {
            if (storeResult.succeeded()) {
                Topic storeTopic = storeResult.result();
                ResourceName resourceName = null;
                if (storeTopic != null) {
                    resourceName = storeTopic.getResourceName();
                } else {
                    resourceName = topicName.asKubeName();
                }
                k8s.getFromName(resourceName, kubeResult -> {
                    if (kubeResult.succeeded()) {
                        KafkaTopic topic = kubeResult.result();
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
                                // resource...
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

    /** Called when a resource is added in k8s */
    void onResourceAdded(KafkaTopic addedTopic, Handler<AsyncResult<Void>> resultHandler) {
        final Topic k8sTopic;
        try {
            k8sTopic = TopicSerialization.fromTopicResource(addedTopic);
        } catch (InvalidTopicException e) {
            resultHandler.handle(Future.failedFuture(e));
            return;
        }
        Handler<Future<Void>> action = new Reconciliation("onResourceAdded") {
            @Override
            public void handle(Future<Void> fut) {
                TopicOperator.this.reconcileOnResourceChange(addedTopic, k8sTopic, false, fut);
            }
        };
        inFlight.enqueue(new TopicName(addedTopic), action, resultHandler);
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

    /** Called when a resource is modified in k8s */
    void onResourceModified(KafkaTopic modifiedTopic, Handler<AsyncResult<Void>> resultHandler) {
        final Topic k8sTopic;
        try {
            k8sTopic = TopicSerialization.fromTopicResource(modifiedTopic);
        } catch (InvalidTopicException e) {
            resultHandler.handle(Future.failedFuture(e));
            return;
        }
        Reconciliation action = new Reconciliation("onResourceModified") {
            @Override
            public void handle(Future<Void> fut) {
                TopicOperator.this.reconcileOnResourceChange(modifiedTopic, k8sTopic, true, fut);
            }
        };
        inFlight.enqueue(new TopicName(modifiedTopic), action, resultHandler);
    }

    private void reconcileOnResourceChange(KafkaTopic topicResource, Topic k8sTopic, boolean isModify, Handler<AsyncResult<Void>> handler) {
        TopicName topicName = new TopicName(topicResource);
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
                    enqueue(new Event(topicResource, "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.", EventType.WARNING, handler));
                } else {
                    reconcile(topicResource, k8sTopic, kafkaTopic, privateTopic, handler);
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    /** Called when a resource is deleted in k8s */
    void onResourceDeleted(KafkaTopic deletedTopic, Handler<AsyncResult<Void>> resultHandler) {
        Reconciliation action = new Reconciliation("onResourceDeleted") {
            @Override
            public void handle(Future<Void> fut) {
                TopicOperator.this.reconcileOnResourceChange(deletedTopic, null, false, fut);
            }
        };
        inFlight.enqueue(new TopicName(deletedTopic), action, resultHandler);
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
        LOGGER.debug("Inflight: {}", inFlight.toString());
        return inFlight.size() > 0;
    }

    /**
     * @return a new instance of BackOff with configured topic metadata max attempts
     */
    private BackOff topicMetadataBackOff() {
        return new BackOff(config.get(Config.TOPIC_METADATA_MAX_ATTEMPTS));
    }

    /**
     * @param resource Resource instance to log
     * @return Resource representation as namespace/name for logging purposes
     */
    static String logTopic(KafkaTopic resource) {
        return resource != null ? resource.getMetadata().getNamespace() + "/" + resource.getMetadata().getName() : null;
    }


    static class ReconcileState {
        private final Set<TopicName> succeeded;
        private final Set<TopicName> undetermined;
        private final Map<TopicName, Throwable> failed;
        private List<KafkaTopic> ktList;

        public ReconcileState(Set<TopicName> succeeded, Set<TopicName> undetermined, Map<TopicName, Throwable> failed) {
            this.succeeded = succeeded;
            this.undetermined = undetermined;
            this.failed = failed;
        }

        public void addKafkaTopics(List<KafkaTopic> ktList) {
            this.ktList = ktList;
        }
    }

    Future<?> reconcileAllTopics(String reconciliationType) {
        LOGGER.info("Starting {} reconciliation", reconciliationType);
        Future<Set<String>> listFut = Future.future();
        kafka.listTopics(listFut.completer());
        return listFut.recover(ex -> Future.failedFuture(
                new OperatorException("Error listing existing topics during " + reconciliationType + " reconciliation", ex)
        )).compose(topicNamesFromKafka ->
                // Reconcile the topic found in Kafka
                reconcileFromKafka(reconciliationType, topicNamesFromKafka.stream().map(TopicName::new).collect(Collectors.toList()))

        ).compose(reconcileState -> {
            Future<List<KafkaTopic>> ktFut = Future.future();
            // Find all the topics in kube
            k8s.listMaps(ktFut.completer());
            return ktFut.recover(ex -> Future.failedFuture(
                    new OperatorException("Error listing existing KafkaTopics during " + reconciliationType + " reconciliation", ex)
            )).map(ktList -> {
                reconcileState.addKafkaTopics(ktList);
                return reconcileState;
            });
        }).compose(reconcileState -> {
            List<Future> futs = new ArrayList<>();
            for (KafkaTopic kt : reconcileState.ktList) {
                Topic topic = TopicSerialization.fromTopicResource(kt);
                TopicName topicName = topic.getTopicName();
                if (reconcileState.failed.containsKey(topicName)) {
                    // we already failed to reconcile this topic in reconcileFromKafka(), /
                    // don't bother trying again
                    LOGGER.trace("Already failed to reconcile {}", topicName);
                } else if (reconcileState.succeeded.contains(topicName)) {
                    // we already succeeded in reconciling this topic in reconcileFromKafka()
                    LOGGER.trace("Already successfully reconciled {}", topicName);
                } else if (reconcileState.undetermined.contains(topicName)) {
                    // The topic didn't exist in topicStore, but now we know which KT it corresponds to
                    futs.add(reconcileWithKubeTopic(reconciliationType, kt, topic).compose(r -> {
                        // if success then remove from undetermined add to success
                        reconcileState.undetermined.remove(topicName);
                        reconcileState.succeeded.add(topicName);
                        return Future.succeededFuture(Boolean.TRUE);
                    }));
                } else {
                    // Topic exists in kube, but not in Kafka
                    LOGGER.debug("Topic {} exists in Kafka, but not Kubernetes", topicName, logTopic(kt));
                    futs.add(reconcileWithKubeTopic(reconciliationType, kt, topic).compose(r -> {
                        // if success then add to success
                        reconcileState.succeeded.add(topicName);
                        return Future.succeededFuture(Boolean.TRUE);
                    }));
                }
            }
            return CompositeFuture.join(futs).compose(joined -> {
                List<Future> futs2 = new ArrayList<>();
                for (Throwable exception : reconcileState.failed.values()) {
                    futs2.add(Future.failedFuture(exception));
                }
                // anything left in undetermined doesn't exist in topic store nor kube
                for (TopicName tn : reconcileState.undetermined) {
                    futs2.add(getKafkaAndReconcile(tn, null, null));
                }
                return CompositeFuture.join(futs2);
            });
        });
    }


    /**
     * Reconcile all the topics in {@code foundFromKafka}, returning a ReconciliationState.
     */
    private Future<ReconcileState> reconcileFromKafka(String reconciliationType, List<TopicName> topicsFromKafka) {
        Set<TopicName> succeeded = new HashSet<>();
        Set<TopicName> undetermined = new HashSet<>();
        Map<TopicName, Throwable> failed = new HashMap<>();

        LOGGER.debug("Reconciling kafka topics {}", topicsFromKafka);

        final ReconcileState state = new ReconcileState(succeeded, undetermined, failed);
        if (topicsFromKafka.size() > 0) {
            List<Future<Void>> futures = new ArrayList<>();
            for (TopicName topicName : topicsFromKafka) {
                Future<Topic> f = Future.future();
                topicStore.read(topicName, f.completer());
                futures.add(f.recover(error -> {
                    failed.put(topicName,
                            new OperatorException("Error getting KafkaTopic " + topicName + " during "
                                    + reconciliationType + " reconciliation", error));
                    return Future.succeededFuture();
                }).compose(topic -> {
                    if (topic == null) {
                        undetermined.add(topicName);
                        return Future.succeededFuture();
                    } else {
                        LOGGER.debug("Have private topic for topic {} in Kafka", topicName);
                        return reconcileWithPrivateTopic(reconciliationType, topicName, topic).otherwise(error -> {
                            failed.put(topicName, error);
                            return null;
                        }).map(ignored -> {
                            succeeded.add(topicName);
                            return null;
                        });
                    }
                }));
            }
            return CompositeFuture.join((List) futures).map(state);
        } else {
            return Future.succeededFuture(state);
        }


    }

    /**
     * Reconcile the given topic which has the given {@code privateTopic} in the topic store.
     */
    private Future<Void> reconcileWithPrivateTopic(String reconciliationType, TopicName topicName, Topic privateTopic) {
        Future<KafkaTopic> kubeFuture = Future.future();
        k8s.getFromName(privateTopic.getResourceName(), kubeFuture.completer());
        return kubeFuture
            .compose(kafkaTopicResource -> getKafkaAndReconcile(topicName, privateTopic, kafkaTopicResource))
            .recover(error -> {
                LOGGER.error("Error {} getting KafkaTopic {} for topic {}",
                        reconciliationType,
                        topicName.asKubeName(), topicName, error);
                return Future.failedFuture(new OperatorException("Error getting KafkaTopic " + topicName.asKubeName() + " during " + reconciliationType + " reconciliation", error));
            });
    }

    private Future<Void> getKafkaAndReconcile(TopicName topicName, Topic privateTopic, KafkaTopic kafkaTopicResource) {
        Future<Void> topicFuture = Future.future();
        Future<Void> result = Future.future();
        Handler<Future<Void>> action = new Reconciliation("reconcile") {
            @Override
            public void handle(Future<Void> fut) {
                try {
                    Topic k8sTopic = kafkaTopicResource != null ? TopicSerialization.fromTopicResource(kafkaTopicResource) : null;
                    kafka.topicMetadata(topicName, metadataResult -> {
                        if (metadataResult.succeeded()) {
                            TopicMetadata kafkaTopicMeta = metadataResult.result();
                            Topic topicFromKafka = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
                            reconcile(kafkaTopicResource, k8sTopic, topicFromKafka, privateTopic, reconcileResult -> {
                                if (reconcileResult.succeeded()) {
                                    LOGGER.info("Success reconciling KafkaTopic {}", logTopic(kafkaTopicResource));
                                    fut.complete();
                                } else {
                                    LOGGER.error("Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), reconcileResult.cause());
                                    fut.fail(reconcileResult.cause());
                                }
                            });
                        } else {
                            LOGGER.error("Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), metadataResult.cause());
                            fut.fail(metadataResult.cause());
                        }
                    });
                } catch (InvalidTopicException e) {
                    LOGGER.error("Error reconciling KafkaTopic {}: Invalid resource: ", logTopic(kafkaTopicResource), e.getMessage());
                    fut.fail(e);
                } catch (OperatorException e) {
                    LOGGER.error("Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), e);
                    fut.fail(e);
                }
            }
        };
        inFlight.enqueue(topicName, action, result);
        result.setHandler(ar -> {
            if (ar.succeeded()) {
                topicFuture.complete();
            } else {
                topicFuture.fail(ar.cause());
            }
        });
        return topicFuture;
    }

    private Future<Void> reconcileWithKubeTopic(String reconciliationType, KafkaTopic resource, Topic k8sTopic) {
        LOGGER.debug("{} reconciliation of KafkaTopic {}", reconciliationType, resource.getMetadata().getName());
        TopicName topicName = k8sTopic.getTopicName();
        Future<Void> result = Future.future();
        Handler<Future<Void>> action = new Reconciliation("reconcile") {
            @Override
            public void handle(Future<Void> fut) {
                try {
                    Future<Topic> topicResult = Future.future();
                    Future<TopicMetadata> metadataResult = Future.future();
                    kafka.topicMetadata(topicName, metadataResult.completer());
                    topicStore.read(topicName, topicResult.completer());
                    CompositeFuture.all(topicResult, metadataResult).setHandler(ar -> {

                        if (ar.succeeded()) {
                            Topic privateTopic = ar.result().resultAt(0);
                            TopicMetadata kafkaTopicMeta = ar.result().resultAt(1);
                            Topic kafkaTopic = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
                            reconcile(resource, k8sTopic, kafkaTopic, privateTopic, reconcileResult -> {
                                if (reconcileResult.succeeded()) {
                                    LOGGER.info("Success reconciling KafkaTopic {}", logTopic(resource));
                                    fut.complete();
                                } else {
                                    LOGGER.error("Error reconciling KafkaTopic {}", logTopic(resource), reconcileResult.cause());
                                    fut.fail(reconcileResult.cause());
                                }
                            });
                        } else {
                            LOGGER.error("Error reconciling KafkaTopic {}", logTopic(resource), ar.cause());
                            fut.fail(ar.cause());
                        }
                    });
                } catch (InvalidTopicException e) {
                    LOGGER.error("Error reconciling KafkaTopic {}: Invalid resource: ", logTopic(resource), e.getMessage());
                    fut.fail(e);
                } catch (OperatorException e) {
                    LOGGER.error("Error reconciling KafkaTopic {}", logTopic(resource), e);
                    fut.fail(e);
                }
            }
        };
        inFlight.enqueue(topicName, action, result);
        return result;
    }

}

