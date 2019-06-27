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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.disjoint;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
class TopicOperator {

    private final static Logger LOGGER = LogManager.getLogger(TopicOperator.class);
    private final static Logger EVENT_LOGGER = LogManager.getLogger("Event");
    private final Kafka kafka;
    private final K8s k8s;
    private final Vertx vertx;
    private final Labels labels;
    private final String namespace;
    private TopicStore topicStore;
    private final Config config;
    private final ConcurrentHashMap<TopicName, Integer> inflight = new ConcurrentHashMap<>();

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
            final String eventTime = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss'Z'"));
            
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
                    .withLastTimestamp(eventTime)
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
        private final LogContext logContext;

        public CreateResource(LogContext logContext, Topic topic, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "CreateResource(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /** Topic deleted in ZK */
    class DeleteResource implements Handler<Void> {

        private final ResourceName resourceName;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;
        private final LogContext logContext;

        public DeleteResource(LogContext logContext, ResourceName resourceName, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.resourceName = resourceName;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            k8s.deleteResource(resourceName, handler);
        }

        @Override
        public String toString() {
            return "DeleteResource(mapName=" + resourceName + ",ctx=" + logContext + ")";
        }
    }

    /** Topic config modified in ZK */
    class UpdateResource implements Handler<Void> {

        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;
        private final LogContext logContext;

        public UpdateResource(LogContext logContext, Topic topic, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "UpdateResource(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /** Resource created in k8s */
    class CreateKafkaTopic implements Handler<Void> {

        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        public CreateKafkaTopic(LogContext logContext, Topic topic,
                                HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            kafka.createTopic(topic, ar -> {
                if (ar.succeeded()) {
                    LOGGER.info("{}: Created topic '{}' for KafkaTopic '{}'", logContext, topic.getTopicName(), topic.getResourceName());
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
            return "CreateKafkaTopic(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /** KafkaTopic modified in k8s */
    class UpdateKafkaConfig implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        public UpdateKafkaConfig(LogContext logContext, Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "UpdateKafkaConfig(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /** KafkaTopic modified in k8s */
    class IncreaseKafkaPartitions implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        public IncreaseKafkaPartitions(LogContext logContext, Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "UpdateKafkaPartitions(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
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
        private final LogContext logContext;

        public DeleteKafkaTopic(LogContext logContext, TopicName topicName, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.topicName = topicName;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws OperatorException {
            LOGGER.info("{}: Deleting topic '{}'", logContext, topicName);
            kafka.deleteTopic(topicName, handler);
        }

        @Override
        public String toString() {
            return "DeleteKafkaTopic(topicName=" + topicName + ",ctx=" + logContext + ")";
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
        this.namespace = namespace;
        this.config = config;
    }

    /**
     * Run the given {@code action} on the context thread,
     * immediately if there are currently no other actions with the given {@code key},
     * or when the other actions with the given {@code key} have completed.
     * When the given {@code action} is complete it must complete its argument future,
     * which will complete the returned future
     */
    public <T> Future<T> executeWithTopicLockHeld(LogContext logContext, TopicName key, Handler<Future<T>> action) {
        String lockName = key.toString();
        int timeoutMs = 30 * 1_000;
        Future<T> result = Future.future();
        BiFunction<TopicName, Integer, Integer> decrement = (topicName, waiters) -> {
            if (waiters != null) {
                if (waiters == 1) {
                    LOGGER.debug("{}: Removing last waiter {}", logContext, action);
                    return null;
                } else {
                    LOGGER.debug("{}: Removing waiter {}, {} waiters left", logContext, action, waiters - 1);
                    return waiters - 1;
                }
            } else {
                LOGGER.error("{}: Assertion failure. topic {}, action {}", logContext, lockName, action);
                return null;
            }
        };
        LOGGER.debug("{}: Queuing action {} on topic {}", logContext, action, lockName);
        inflight.compute(key, (topicName, waiters) -> {
            if (waiters == null) {
                LOGGER.debug("{}: Adding first waiter {}", logContext, action);
                return 1;
            } else {
                LOGGER.debug("{}: Adding waiter {}: {}", logContext, action, waiters + 1);
                return waiters + 1;
            }
        });
        vertx.sharedData().getLockWithTimeout(lockName, timeoutMs, ar -> {
            if (ar.succeeded()) {
                LOGGER.debug("{}: Lock acquired", logContext);
                Future<T> f = Future.future();
                f.setHandler(ar2 -> {
                    LOGGER.debug("{}: Executing handler for action {} on topic {}", logContext, action, lockName);
                    try {
                        result.handle(ar2);
                    } finally {
                        ar.result().release();
                        LOGGER.debug("{}: Lock released", logContext);
                        inflight.compute(key, decrement);
                    }
                });
                LOGGER.debug("{}: Executing action {} on topic {}", logContext, action, lockName);
                action.handle(f);
            } else {
                LOGGER.warn("{}: Lock not acquired within {}ms: action {} will not be run", logContext, timeoutMs, action);
                try {
                    result.handle(Future.failedFuture("Failed to acquire lock for topic " + lockName + " after " + timeoutMs + "ms. Not executing action " + action));
                } finally {
                    inflight.compute(key, decrement);
                }
            }
        });
        return result;
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
    void reconcile(final LogContext logContext, final HasMetadata involvedObject,
                   final Topic k8sTopic, final Topic kafkaTopic, final Topic privateTopic,
                   final Handler<AsyncResult<Void>> reconciliationResultHandler) {

        {
            TopicName topicName = k8sTopic != null ? k8sTopic.getTopicName() : kafkaTopic != null ? kafkaTopic.getTopicName() : privateTopic != null ? privateTopic.getTopicName() : null;
            LOGGER.info("{}: Reconciling topic {}, k8sTopic:{}, kafkaTopic:{}, privateTopic:{}", logContext, topicName, k8sTopic == null ? "null" : "nonnull", kafkaTopic == null ? "null" : "nonnull", privateTopic == null ? "null" : "nonnull");
        }
        if (privateTopic == null) {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // All three null: This happens reentrantly when a topic or KafkaTopic is deleted
                    LOGGER.debug("{}: All three topics null during reconciliation.", logContext);
                    reconciliationResultHandler.handle(Future.succeededFuture());
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    LOGGER.debug("{}: topic created in kafka, will create KafkaTopic in k8s and topicStore", logContext);
                    enqueue(new CreateResource(logContext, kafkaTopic, ar -> {
                        // In all cases, create in privateState
                        if (ar.succeeded()) {
                            enqueue(new CreateInTopicStore(logContext, kafkaTopic, involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar);
                        }
                    }));
                }
            } else if (kafkaTopic == null) {
                // it's been created in k8s => create in Kafka and privateState
                LOGGER.debug("{}: KafkaTopic created in k8s, will create topic in kafka and topicStore", logContext);
                enqueue(new CreateKafkaTopic(logContext, k8sTopic, involvedObject, ar -> {
                    // In all cases, create in privateState
                    if (ar.succeeded()) {
                        enqueue(new CreateInTopicStore(logContext, k8sTopic, involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            } else {
                update2Way(logContext, involvedObject, k8sTopic, kafkaTopic, reconciliationResultHandler);
            }
        } else {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // delete privateState
                    LOGGER.debug("{}: KafkaTopic deleted in k8s and topic deleted in kafka => delete from topicStore", logContext);
                    enqueue(new DeleteFromTopicStore(logContext, privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    LOGGER.debug("{}: KafkaTopic deleted in k8s => delete topic from kafka and from topicStore", logContext);
                    enqueue(new DeleteKafkaTopic(logContext, kafkaTopic.getTopicName(), ar -> {
                        if (ar.succeeded()) {
                            enqueue(new DeleteFromTopicStore(logContext, privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar);
                        }
                    }));

                }
            } else if (kafkaTopic == null) {
                // it was deleted in kafka so delete in k8s and privateState
                LOGGER.debug("{}: topic deleted in kafkas => delete KafkaTopic from k8s and from topicStore", logContext);
                enqueue(new DeleteResource(logContext, privateTopic.getOrAsKubeName(), ar -> {
                    if (ar.succeeded()) {
                        enqueue(new DeleteFromTopicStore(logContext, privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            } else {
                // all three exist
                LOGGER.debug("{}: 3 way diff", logContext);
                update3Way(logContext, involvedObject, k8sTopic, kafkaTopic, privateTopic, reconciliationResultHandler);
            }
        }
    }

    private void update2Way(LogContext logContext, HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Handler<AsyncResult<Void>> reconciliationResultHandler) {
        TopicDiff diff = TopicDiff.diff(kafkaTopic, k8sTopic);
        if (diff.isEmpty()) {
            // they're the same => do nothing, but stil create the private copy
            LOGGER.debug("{}: KafkaTopic created in k8s and topic created in kafka, but they're identical => just creating in topicStore", logContext);
            LOGGER.debug("{}: k8s and kafka versions of topic '{}' are the same", logContext, kafkaTopic.getTopicName());
            enqueue(new CreateInTopicStore(logContext, kafkaTopic, involvedObject, reconciliationResultHandler));
        } else if (!diff.changesReplicationFactor()
                && !diff.changesNumPartitions()
                && diff.changesConfig()
                && disjoint(kafkaTopic.getConfig().keySet(), k8sTopic.getConfig().keySet())) {
            LOGGER.debug("{}: KafkaTopic created in k8s and topic created in kafka, they differ only in topic config, and those configs are disjoint: Updating k8s and kafka, and creating in topic store", logContext);
            Map<String, String> mergedConfigs = new HashMap<>(kafkaTopic.getConfig());
            mergedConfigs.putAll(k8sTopic.getConfig());
            Topic mergedTopic = new Topic.Builder(kafkaTopic).withConfig(mergedConfigs).build();
            enqueue(new UpdateResource(logContext, mergedTopic, ar -> {
                if (ar.succeeded()) {
                    enqueue(new UpdateKafkaConfig(logContext, mergedTopic, involvedObject, ar2 -> {
                        if (ar2.succeeded()) {
                            enqueue(new CreateInTopicStore(logContext, mergedTopic, involvedObject, reconciliationResultHandler));
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
            LOGGER.debug("{}: KafkaTopic created in k8s and topic created in kafka, and they are irreconcilably different => kafka version wins", logContext);
            enqueue(new Event(involvedObject, "KafkaTopic is incompatible with the topic metadata. " +
                    "The topic metadata will be treated as canonical.", EventType.INFO, ar -> {
                if (ar.succeeded()) {
                    enqueue(new UpdateResource(logContext, kafkaTopic, ar2 -> {
                        if (ar2.succeeded()) {
                            enqueue(new CreateInTopicStore(logContext, kafkaTopic, involvedObject, reconciliationResultHandler));
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

    private void update3Way(LogContext logContext, HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Topic privateTopic,
                            Handler<AsyncResult<Void>> reconciliationResultHandler) {
        if (!privateTopic.getResourceName().equals(k8sTopic.getResourceName())) {
            reconciliationResultHandler.handle(Future.failedFuture(new OperatorException(involvedObject,
                    "Topic '" + kafkaTopic.getTopicName() + "' is already managed via KafkaTopic '" + privateTopic.getResourceName() + "' it cannot also be managed via the KafkaTopic '" + k8sTopic.getResourceName() + "'")));
            return;
        }
        TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
        LOGGER.debug("{}: topicStore->kafkaTopic: {}", logContext, oursKafka);
        TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
        LOGGER.debug("{}: topicStore->k8sTopic: {}", logContext, oursK8s);
        String conflict = oursKafka.conflict(oursK8s);
        if (conflict != null) {
            final String message = "KafkaTopic resource and Kafka topic both changed in a conflicting way: " + conflict;
            LOGGER.error("{}: {}", logContext, message);
            enqueue(new Event(involvedObject, message, EventType.INFO, eventResult -> { }));
            reconciliationResultHandler.handle(Future.failedFuture(new Exception(message)));
        } else {
            TopicDiff merged = oursKafka.merge(oursK8s);
            LOGGER.debug("{}: Diffs do not conflict, merged diff: {}", logContext, merged);
            if (merged.isEmpty()) {
                LOGGER.info("{}: All three topics are identical", logContext);
                reconciliationResultHandler.handle(Future.succeededFuture());
            } else {
                Topic result = merged.apply(privateTopic);
                int partitionsDelta = merged.numPartitionsDelta();
                if (partitionsDelta < 0) {
                    final String message = "Number of partitions cannot be decreased";
                    LOGGER.error("{}: {}", logContext, message);
                    enqueue(new Event(involvedObject, message, EventType.INFO, eventResult -> {
                    }));
                    reconciliationResultHandler.handle(Future.failedFuture(new Exception(message)));
                } else {
                    if (merged.changesReplicationFactor()) {
                        LOGGER.error("{}: Changes replication factor", logContext);
                        enqueue(new ChangeReplicationFactor(result, involvedObject, res -> LOGGER.error(
                                "Changing replication factor is not supported")));
                    }
                    // TODO What if we increase min.in.sync.replicas and the number of replicas,
                    // such that the old number of replicas < the new min isr? But likewise
                    // we could decrease, so order of tasks in the queue will need to change
                    // depending on what the diffs are.
                    LOGGER.debug("{}: Updating KafkaTopic, kafka topic and topicStore", logContext);
                    // TODO replace this with compose
                    TopicDiff diff = TopicDiff.diff(k8sTopic, result);
                    if (diff.isEmpty()) {
                        LOGGER.debug("{}: No need to update KafkaTopic with {}", logContext, diff);
                        enqueue(updateTopicStoreAndKafka(logContext, involvedObject, reconciliationResultHandler, merged, result, partitionsDelta));
                    } else {
                        LOGGER.debug("{}: Updating KafkaTopic with {}", logContext, diff);
                        UpdateResource event = new UpdateResource(logContext, result, ar -> {
                            enqueue(updateTopicStoreAndKafka(logContext, involvedObject, reconciliationResultHandler, merged, result, partitionsDelta));
                        });
                        enqueue(event);
                    }
                }
            }
        }
    }

    private Handler<Void> updateTopicStoreAndKafka(LogContext logContext, HasMetadata involvedObject, Handler<AsyncResult<Void>> reconciliationResultHandler, TopicDiff merged, Topic topic, int partitionsDelta) {
        Handler<Void> topicStoreHandler =
            ignored -> enqueue(new UpdateInTopicStore(logContext, topic, involvedObject, reconciliationResultHandler));
        Handler<Void> partitionsHandler;
        if (partitionsDelta > 0) {
            partitionsHandler = ar4 -> enqueue(new IncreaseKafkaPartitions(logContext, topic, involvedObject, ar2 -> topicStoreHandler.handle(null)));
        } else {
            partitionsHandler = topicStoreHandler;
        }

        Handler<Void> result;
        if (merged.changesConfig()) {
            result = new UpdateKafkaConfig(logContext, topic, involvedObject, ar2 -> partitionsHandler.handle(null));
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
    void onTopicDeleted(LogContext logContext, TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        Handler<Future<Void>> action = new Reconciliation("onTopicDeleted") {
            @Override
            public void handle(Future<Void> fut) {
                TopicOperator.this.reconcileOnTopicChange(logContext, topicName, null, fut);
            }
        };
        executeWithTopicLockHeld(logContext, topicName, action).setHandler(resultHandler);

    }

    /**
     * Called when ZK watch notifies of change to topic's config
     */
    void onTopicConfigChanged(LogContext logContext, TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        Handler<Future<Void>> action = new Reconciliation("onTopicConfigChanged") {
            @Override
            public void handle(Future<Void> fut) {
                kafka.topicMetadata(topicName, metadataResult -> {
                    if (metadataResult.succeeded()) {
                        Topic topic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                        TopicOperator.this.reconcileOnTopicChange(logContext, topicName, topic, fut);
                    } else {
                        fut.fail(metadataResult.cause());
                    }
                });
            }
        };
        executeWithTopicLockHeld(logContext, topicName, action).setHandler(resultHandler);
    }

    /**
     * Called when ZK watch notifies of a change to the topic's partitions
     */
    void onTopicPartitionsChanged(LogContext logContext, TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
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
                                        TopicOperator.this.reconcileOnTopicChange(logContext, topicName, kafkaTopic, fut);
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
        executeWithTopicLockHeld(logContext, topicName, action).setHandler(resultHandler);
    }

    /**
     * Called when one of the ZK watches notifies of a change to the topic
     */
    private void reconcileOnTopicChange(LogContext logContext, TopicName topicName, Topic kafkaTopic, Handler<AsyncResult<Void>> resultHandler) {
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
                        reconcile(logContext.withKubeTopic(topic), topic, k8sTopic, kafkaTopic, storeTopic, resultHandler);
                    } else {
                        resultHandler.handle(kubeResult.map((Void) null));
                    }
                });
            } else {
                resultHandler.handle(storeResult.map((Void) null));
            }
        });
    }

    /** Called when a topic znode is created in ZK */
    void onTopicCreated(LogContext logContext, TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
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
                                reconcileOnTopicChange(logContext, topicName, kafkaTopic, fut);
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
        executeWithTopicLockHeld(logContext, topicName, action).setHandler(resultHandler);
    }

    /** Called when a resource is added in k8s */
    void onResourceAdded(LogContext logContext, KafkaTopic addedTopic, Handler<AsyncResult<Void>> resultHandler) {
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
                TopicOperator.this.reconcileOnResourceChange(logContext, addedTopic, k8sTopic, false, fut);
            }
        };
        executeWithTopicLockHeld(logContext, new TopicName(addedTopic), action).setHandler(resultHandler);
    }

    abstract class Reconciliation implements Handler<Future<Void>> {
        private final String name;

        public Reconciliation(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    };

    /** Called when a resource is modified in k8s */
    void onResourceModified(LogContext logContext, KafkaTopic modifiedTopic, Handler<AsyncResult<Void>> resultHandler) {
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
                TopicOperator.this.reconcileOnResourceChange(logContext, modifiedTopic, k8sTopic, true, fut);
            }
        };
        executeWithTopicLockHeld(logContext, new TopicName(modifiedTopic), action).setHandler(resultHandler);
    }

    private void reconcileOnResourceChange(LogContext logContext, KafkaTopic topicResource, Topic k8sTopic,
                                           boolean isModify, Handler<AsyncResult<Void>> handler) {
        TopicName topicName = new TopicName(topicResource);
        CompositeFuture.all(getFromKafka(topicName), getFromTopicStore(topicName)).setHandler(ar -> {
            if (ar.succeeded()) {
                Topic kafkaTopic = ar.result().resultAt(0);
                Topic privateTopic = ar.result().resultAt(1);
                if (privateTopic == null && isModify) {
                    enqueue(new Event(topicResource,
                            "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.",
                            EventType.WARNING, handler));
                } else {
                    reconcile(logContext, topicResource, k8sTopic, kafkaTopic, privateTopic, handler);
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    /** Called when a resource is deleted in k8s */
    void onResourceDeleted(LogContext logContext, KafkaTopic deletedTopic, Handler<AsyncResult<Void>> resultHandler) {
        Reconciliation action = new Reconciliation("onResourceDeleted") {
            @Override
            public void handle(Future<Void> fut) {
                TopicOperator.this.reconcileOnResourceChange(logContext, deletedTopic, null, false, fut);
            }
        };
        executeWithTopicLockHeld(logContext, new TopicName(deletedTopic), action).setHandler(resultHandler);
    }

    private class UpdateInTopicStore implements Handler<Void> {
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        public UpdateInTopicStore(LogContext logContext, Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "UpdateInTopicStore(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    class CreateInTopicStore implements Handler<Void> {
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        private CreateInTopicStore(LogContext logContext, Topic topic, HasMetadata involvedObject,
                                   Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "CreateInTopicStore(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    class DeleteFromTopicStore implements Handler<Void> {
        private final TopicName topicName;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        private DeleteFromTopicStore(LogContext logContext, TopicName topicName, HasMetadata involvedObject,
                                     Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
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
            return "DeleteFromTopicStore(topicName=" + topicName + ",ctx=" + logContext + ")";
        }
    }

    public boolean isWorkInflight() {
        LOGGER.debug("Outstanding: {}", inflight);
        return inflight.size() > 0;
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
    static String logTopic(HasMetadata resource) {
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
        kafka.listTopics(listFut);
        return listFut.recover(ex -> Future.failedFuture(
                new OperatorException("Error listing existing topics during " + reconciliationType + " reconciliation", ex)
        )).compose(topicNamesFromKafka ->
                // Reconcile the topic found in Kafka
                reconcileFromKafka(reconciliationType, topicNamesFromKafka.stream().map(TopicName::new).collect(Collectors.toList()))

        ).compose(reconcileState -> {
            Future<List<KafkaTopic>> ktFut = Future.future();
            // Find all the topics in kube
            k8s.listMaps(ktFut);
            return ktFut.recover(ex -> Future.failedFuture(
                    new OperatorException("Error listing existing KafkaTopics during " + reconciliationType + " reconciliation", ex)
            )).map(ktList -> {
                reconcileState.addKafkaTopics(ktList);
                return reconcileState;
            });
        }).compose(reconcileState -> {
            List<Future> futs = new ArrayList<>();
            for (KafkaTopic kt : reconcileState.ktList) {
                LogContext logContext = LogContext.periodic(reconciliationType + "kube " + kt.getMetadata().getName()).withKubeTopic(kt);
                Topic topic = TopicSerialization.fromTopicResource(kt);
                TopicName topicName = topic.getTopicName();
                if (reconcileState.failed.containsKey(topicName)) {
                    // we already failed to reconcile this topic in reconcileFromKafka(), /
                    // don't bother trying again
                    LOGGER.trace("{}: Already failed to reconcile {}", logContext, topicName);
                } else if (reconcileState.succeeded.contains(topicName)) {
                    // we already succeeded in reconciling this topic in reconcileFromKafka()
                    LOGGER.trace("{}: Already successfully reconciled {}", logContext, topicName);
                } else if (reconcileState.undetermined.contains(topicName)) {
                    // The topic didn't exist in topicStore, but now we know which KT it corresponds to
                    futs.add(reconcileWithKubeTopic(logContext, kt, reconciliationType, new ResourceName(kt), topic.getTopicName()).compose(r -> {
                        // if success then remove from undetermined add to success
                        reconcileState.undetermined.remove(topicName);
                        reconcileState.succeeded.add(topicName);
                        return Future.succeededFuture(Boolean.TRUE);
                    }));
                } else {
                    // Topic exists in kube, but not in Kafka
                    LOGGER.debug("{}: Topic {} exists in Kafka, but not Kubernetes", logContext, topicName, logTopic(kt));
                    futs.add(reconcileWithKubeTopic(logContext, kt, reconciliationType, new ResourceName(kt), topic.getTopicName()).compose(r -> {
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
                    LogContext logContext = LogContext.periodic(reconciliationType + "-" + tn);
                    futs2.add(executeWithTopicLockHeld(logContext, tn, new Reconciliation("delete-remaining") {
                        @Override
                        public void handle(Future<Void> event) {
                            getKafkaAndReconcile(logContext, tn, null, null).setHandler(event);
                        }
                    }));
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
                LogContext logContext = LogContext.periodic(reconciliationType + "kafka " + topicName);
                futures.add(executeWithTopicLockHeld(logContext, topicName, new Reconciliation("reconcile-from-kafka") {
                    @Override
                    public void handle(Future<Void> fut) {
                        getFromTopicStore(topicName).recover(error -> {
                            failed.put(topicName,
                                    new OperatorException("Error getting KafkaTopic " + topicName + " during "
                                            + reconciliationType + " reconciliation", error));
                            return Future.succeededFuture();
                        }).compose(topic -> {
                            if (topic == null) {
                                undetermined.add(topicName);
                                return Future.succeededFuture();
                            } else {
                                LOGGER.debug("{}: Have private topic for topic {} in Kafka", logContext, topicName);
                                return reconcileWithPrivateTopic(logContext, topicName, topic).otherwise(error -> {
                                    failed.put(topicName, error);
                                    return null;
                                }).map(ignored -> {
                                    succeeded.add(topicName);
                                    return null;
                                });
                            }
                        }).map(i ->  {
                            fut.complete();
                            return null;
                        });
                    }
                }));
            }
            return join(futures).map(state);
        } else {
            return Future.succeededFuture(state);
        }


    }

    @SuppressWarnings("unchecked")
    private static <T> CompositeFuture join(List<T> futures) {
        return CompositeFuture.join((List) futures);
    }


    /**
     * Reconcile the given topic which has the given {@code privateTopic} in the topic store.
     */
    private Future<Void> reconcileWithPrivateTopic(LogContext logContext, TopicName topicName, Topic privateTopic) {
        Future<KafkaTopic> kubeFuture = Future.future();
        k8s.getFromName(privateTopic.getResourceName(), kubeFuture);
        return kubeFuture
            .compose(kafkaTopicResource -> {
                return getKafkaAndReconcile(logContext, topicName, privateTopic, kafkaTopicResource);
            })
            .recover(error -> {
                LOGGER.error("{}: Error getting KafkaTopic {} for topic {}",
                        logContext,
                        topicName.asKubeName(), topicName, error);
                return Future.failedFuture(new OperatorException("Error getting KafkaTopic " + topicName.asKubeName() + " during " + logContext.trigger() + " reconciliation", error));
            });
    }

    private Future<Void> getKafkaAndReconcile(LogContext logContext, TopicName topicName, Topic privateTopic, KafkaTopic kafkaTopicResource) {
        logContext.withKubeTopic(kafkaTopicResource);
        Future<Void> topicFuture = Future.future();
        try {
            Topic k8sTopic = kafkaTopicResource != null ? TopicSerialization.fromTopicResource(kafkaTopicResource) : null;
            kafka.topicMetadata(topicName, metadataResult -> {
                if (metadataResult.succeeded()) {
                    TopicMetadata kafkaTopicMeta = metadataResult.result();
                    Topic topicFromKafka = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
                    reconcile(logContext, kafkaTopicResource, k8sTopic, topicFromKafka, privateTopic, reconcileResult -> {
                        if (reconcileResult.succeeded()) {
                            LOGGER.info("Success reconciling KafkaTopic {}", logTopic(kafkaTopicResource));
                            topicFuture.complete();
                        } else {
                            LOGGER.error("Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), reconcileResult.cause());
                            topicFuture.fail(reconcileResult.cause());
                        }
                    });
                } else {
                    LOGGER.error("Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), metadataResult.cause());
                    topicFuture.fail(metadataResult.cause());
                }
            });
        } catch (InvalidTopicException e) {
            LOGGER.error("Error reconciling KafkaTopic {}: Invalid resource: ", logTopic(kafkaTopicResource), e.getMessage());
            topicFuture.fail(e);
        } catch (OperatorException e) {
            LOGGER.error("Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), e);
            topicFuture.fail(e);
        }
        return topicFuture;
    }

    Future<KafkaTopic> getFromKube(ResourceName kubeName) {
        Future<KafkaTopic> f = Future.future();
        k8s.getFromName(kubeName, f);
        return f;
    }

    Future<Topic> getFromKafka(TopicName topicName) {
        Future<TopicMetadata> f = Future.future();
        kafka.topicMetadata(topicName, f);
        return f.map(TopicSerialization::fromTopicMetadata);
    }

    Future<Topic> getFromTopicStore(TopicName topicName) {
        Future<Topic> f = Future.future();
        topicStore.read(topicName, f);
        return f;
    }

    private Future<Void> reconcileWithKubeTopic(LogContext logContext, HasMetadata involvedObject, String reconciliationType, ResourceName kubeName, TopicName topicName) {
        Future<Void> result = Future.future();
        executeWithTopicLockHeld(logContext, topicName, new Reconciliation("reconcile-with-kube") {
            @Override
            public void handle(Future<Void> fut) {
                CompositeFuture.join(getFromKube(kubeName),
                        getFromKafka(topicName),
                        getFromTopicStore(topicName))
                    .setHandler(ar -> {
                        if (ar.succeeded()) {
                            KafkaTopic ktr = ar.result().resultAt(0);
                            logContext.withKubeTopic(ktr);
                            Topic k8sTopic = TopicSerialization.fromTopicResource(ktr);
                            Topic kafkaTopic = ar.result().resultAt(1);
                            Topic privateTopic = ar.result().resultAt(2);
                            reconcile(logContext, involvedObject, k8sTopic, kafkaTopic, privateTopic, reconcileResult -> {
                                if (reconcileResult.succeeded()) {
                                    LOGGER.info("{}: Success reconciling KafkaTopic {}", logContext, logTopic(involvedObject));
                                    fut.complete();
                                } else {
                                    LOGGER.error("{}: Error reconciling KafkaTopic {}", logContext, logTopic(involvedObject), reconcileResult.cause());
                                    fut.fail(reconcileResult.cause());
                                }
                            });
                        } else {
                            LOGGER.error("{}: Error reconciling KafkaTopic {}", logContext, kubeName, ar.cause());
                            fut.fail(ar.cause());
                        }
                    });
            }
        }).setHandler(result);
        return result;
    }

}

