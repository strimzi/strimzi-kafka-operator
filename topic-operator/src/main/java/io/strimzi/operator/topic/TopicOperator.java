/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.Watcher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Meter;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.disjoint;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/** An operator which manages the whole Topic operator operations */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
class TopicOperator {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperator.class);

    public static final String METRICS_PREFIX = "strimzi.";
    private final Kafka kafka;
    private final K8s k8s;
    private final Vertx vertx;
    private final Labels labels;
    private final String namespace;
    private final TopicStore topicStore;
    private final Config config;
    private final ConcurrentHashMap<TopicName, Integer> inflight = new ConcurrentHashMap<>();

    protected final MetricsProvider metrics;
    private Counter periodicReconciliationsCounter;
    private Counter reconciliationsCounter;
    private Counter failedReconciliationsCounter;
    private Counter successfulReconciliationsCounter;
    private Counter lockedReconciliationsCounter;
    private AtomicInteger topicCounter;
    protected AtomicInteger pausedTopicCounter;
    protected Timer reconciliationsTimer;

    enum EventType {
        INFO("Info"),
        WARNING("Warning");
        final String name;
        EventType(String name) {
            this.name = name;
        }
    }

    class Event implements Handler<Void> {
        private final LogContext logContext;
        private final EventType eventType;
        private final String message;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param involvedObject     Involved Kubernetes resource with metadata
         * @param message            Message related to Event
         * @param eventType          Type of K8s event
         * @param handler            Handles the events
         */
        public Event(LogContext logContext, HasMetadata involvedObject, String message, EventType eventType, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.involvedObject = involvedObject;
            this.message = message;
            this.handler = handler;
            this.eventType = eventType;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) {
            EventBuilder evtb = new EventBuilder();
            final String eventTime = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
            
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
                    LOGGER.infoCr(logContext.toReconciliation(), message);
                    break;
                case WARNING:
                    LOGGER.warnCr(logContext.toReconciliation(), message);
                    break;
            }
            k8s.createEvent(event).onComplete(handler);
        }

        /**
         * @return The Kubernetes resource and correlated error message
         */
        public String toString() {
            return "ErrorEvent(involvedObject=" + involvedObject + ", message=" + message + ")";
        }
    }

    private Future<KafkaTopic> createResource(LogContext logContext, Topic topic) {
        Promise<KafkaTopic> result = Promise.promise();
        enqueue(logContext, new CreateResource(logContext, topic, result));
        return result.future();
    }

    /** Topic created in ZK */
    class CreateResource implements Handler<Void> {
        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<KafkaTopic>> handler;
        private final LogContext logContext;

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param handler            Handles the events
         */
        public CreateResource(LogContext logContext, Topic topic, Handler<io.vertx.core.AsyncResult<KafkaTopic>> handler) {
            this.logContext = logContext;
            this.topic = topic;
            this.handler = handler;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) throws OperatorException {
            KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(this.topic, labels);
            k8s.createResource(kafkaTopic).onComplete(handler);
        }

        /**
         * @return The name and correlated log message of the created topic
         */
        @Override
        public String toString() {
            return "CreateResource(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /**
     * Deletes the topic resource in ZK
     *
     * @param logContext   Instance of LogContext
     * @param resourceName Name of the resource to be deleted
     * @return Future based upon deletion of the resource
     */
    private Future<Void> deleteResource(LogContext logContext, ResourceName resourceName) {
        Promise<Void> result = Promise.promise();
        enqueue(logContext, new DeleteResource(logContext, resourceName, result));
        return result.future();
    }

    /** Topic deleted in ZK */
    class DeleteResource implements Handler<Void> {

        private final ResourceName resourceName;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;
        private final LogContext logContext;

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param resourceName       Name of the resource
         * @param handler            Handles the events
         */
        public DeleteResource(LogContext logContext, ResourceName resourceName, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.resourceName = resourceName;
            this.handler = handler;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) {
            k8s.deleteResource(logContext.toReconciliation(), resourceName).onComplete(handler);
        }

        /**
         * @return The name and correlated log message of the deleted topic
         */
        @Override
        public String toString() {
            return "DeleteResource(mapName=" + resourceName + ",ctx=" + logContext + ")";
        }
    }

    /**
     * Update the topic resource in ZK
     *
     * @param logContext   Instance of LogContext
     * @param topic        The topic whose configs needs to be updated
     * @return Future based upon update of the resource
     */
    private Future<KafkaTopic> updateResource(LogContext logContext, Topic topic) {
        Promise<KafkaTopic> result = Promise.promise();
        enqueue(logContext, new UpdateResource(logContext, topic, result));
        return result.future();
    }

    /** Topic config modified in ZK */
    class UpdateResource implements Handler<Void> {

        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<KafkaTopic>> handler;
        private final LogContext logContext;

        /** Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param topic              The topic whose configs needs to be updated
         * @param handler            Handles the events
         */
        public UpdateResource(LogContext logContext, Topic topic, Handler<AsyncResult<KafkaTopic>> handler) {
            this.logContext = logContext;
            this.topic = topic;
            this.handler = handler;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) {
            KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(this.topic, labels);
            k8s.updateResource(kafkaTopic).onComplete(handler);
        }

        /**
         * @return The name and correlated log message of the updated topic
         */
        @Override
        public String toString() {
            return "UpdateResource(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /**
     * Creates the topic resource in k8s
     *
     * @param logContext      Instance of LogContext
     * @param topic           The topic whose configs needs to be updated
     * @param involvedObject  Involved kubernetes object
     * @return Future based upon creation of the resource
     */
    private Future<Void> createKafkaTopic(LogContext logContext, Topic topic,
                                          HasMetadata involvedObject) {
        Promise<Void> result = Promise.promise();
        enqueue(logContext, new CreateKafkaTopic(logContext, topic, involvedObject, result));
        return result.future();
    }

    /** Resource created in k8s */
    private class CreateKafkaTopic implements Handler<Void> {

        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param topic              The topic whose configs needs to be updated
         * @param involvedObject  Involved kubernetes object
         * @param handler            Handles the events
         */
        public CreateKafkaTopic(LogContext logContext, Topic topic,
                                HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) throws OperatorException {
            kafka.createTopic(logContext.toReconciliation(), topic).onComplete(ar -> {
                if (ar.succeeded()) {
                    LOGGER.debugCr(logContext.toReconciliation(), "Created topic '{}' for KafkaTopic '{}'",
                            topic.getTopicName(), topic.getResourceName());
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

        /**
         * @return The name and correlated log context of the created topic in k8s
         */
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

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param topic              The topic whose configs needs to be updated
         * @param involvedObject  Involved kubernetes object
         * @param handler            Handles the events
         */
        public UpdateKafkaConfig(LogContext logContext, Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) throws OperatorException {
            kafka.updateTopicConfig(logContext.toReconciliation(), topic).onComplete(ar -> {
                if (ar.failed()) {
                    enqueue(logContext, new Event(logContext, involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });

        }

        /**
         * @return The name and correlated log context of the updated topic in k8s
         */
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

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param topic              The topic whose configs needs to be updated
         * @param involvedObject  Involved kubernetes object
         * @param handler            Handles the events
         */
        public IncreaseKafkaPartitions(LogContext logContext, Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) throws OperatorException {
            kafka.increasePartitions(logContext.toReconciliation(), topic).onComplete(ar -> {
                if (ar.failed()) {
                    enqueue(logContext, new Event(logContext, involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });

        }

        /**
         * @return The name and correlated log context of the topic in k8s whose partition are increased
         */
        @Override
        public String toString() {
            return "UpdateKafkaPartitions(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    /**
     * Creates the topic resource in k8s
     *
     * @param logContext      Instance of LogContext
     * @param topicName       Name of the topic to be deleted
     * @return Future based upon deletion of the resource
     */
    private Future<Void> deleteKafkaTopic(LogContext logContext, TopicName topicName) {
        Promise<Void> result = Promise.promise();
        enqueue(logContext, new DeleteKafkaTopic(logContext, topicName, result));
        return result.future();
    }

    /** KafkaTopic deleted in k8s */
    class DeleteKafkaTopic implements Handler<Void> {

        public final TopicName topicName;
        private final Handler<AsyncResult<Void>> handler;
        private final LogContext logContext;

        /**
         * Constructor
         *
         * @param logContext         The context for correlating the logs
         * @param topicName       Name of the topic to be deleted
         * @param handler            Handles the events
         */
        public DeleteKafkaTopic(LogContext logContext, TopicName topicName, Handler<AsyncResult<Void>> handler) {
            this.logContext = logContext;
            this.topicName = topicName;
            this.handler = handler;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(Void v) throws OperatorException {
            LOGGER.infoCr(logContext.toReconciliation(), "Deleting topic '{}'", topicName);
            kafka.deleteTopic(logContext.toReconciliation(), topicName).onComplete(handler);
        }

        /**
         * @return The name and correlated log context of the topic deleted in k8s
         */
        @Override
        public String toString() {
            return "DeleteKafkaTopic(topicName=" + topicName + ",ctx=" + logContext + ")";
        }
    }

    /** Topic Operator constructor */
    protected TopicOperator(Vertx vertx, Kafka kafka,
                         K8s k8s,
                         TopicStore topicStore,
                         Labels labels,
                         String namespace,
                         Config config,
                         MetricsProvider metrics) {
        this.kafka = kafka;
        this.k8s = k8s;
        this.vertx = vertx;
        this.labels = labels;
        this.topicStore = topicStore;
        this.namespace = namespace;
        this.config = config;
        this.metrics = metrics;

        initMetrics();
    }

    protected void initMetrics() {
        if (metrics != null) {
            Tags metricTags = Tags.of(Tag.of("kind", "KafkaTopic"));

            periodicReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.periodical",
                    "Number of periodical reconciliations done by the operator",
                    metricTags);

            reconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations",
                    "Number of reconciliations done by the operator for individual topics",
                    metricTags);

            failedReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.failed",
                    "Number of reconciliations done by the operator for individual topics which failed",
                    metricTags);

            successfulReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.successful",
                    "Number of reconciliations done by the operator for individual topics which were successful",
                    metricTags);

            topicCounter = metrics.gauge(METRICS_PREFIX + "resources",
                    "Number of topics the operator sees",
                    metricTags);

            reconciliationsTimer = metrics.timer(METRICS_PREFIX + "reconciliations.duration",
                    "The time the reconciliation takes to complete",
                    metricTags);

            pausedTopicCounter = metrics.gauge(METRICS_PREFIX + "resources.paused",
                    "Number of topics the operator sees but does not reconcile due to paused reconciliations",
                    metricTags);

            lockedReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.locked",
                    "Number of reconciliations skipped because another reconciliation for the same topic was still running",
                    metricTags);
        }
    }

    protected Counter getPeriodicReconciliationsCounter() {
        return this.periodicReconciliationsCounter;
    }

    /**
     * Run the given {@code action} on the context thread,
     * immediately if there are currently no other actions with the given {@code key},
     * or when the other actions with the given {@code key} have completed.
     * When the given {@code action} is complete it must complete its argument future,
     * which will complete the returned future
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public Future<Void> executeWithTopicLockHeld(LogContext logContext, TopicName key, Reconciliation action) {
        String lockName = key.toString();
        int timeoutMs = 30 * 1_000;
        Promise<Void> result = Promise.promise();
        BiFunction<TopicName, Integer, Integer> decrement = (topicName, waiters) -> {
            if (waiters != null) {
                if (waiters == 1) {
                    LOGGER.debugCr(logContext.toReconciliation(), "Removing last waiter {}", action);
                    return null;
                } else {
                    LOGGER.debugCr(logContext.toReconciliation(), "Removing waiter {}, {} waiters left", action, waiters - 1);
                    return waiters - 1;
                }
            } else {
                LOGGER.errorCr(logContext.toReconciliation(), "Assertion failure. topic {}, action {}", lockName, action);
                return null;
            }
        };
        LOGGER.debugCr(logContext.toReconciliation(), "Queuing action {} on topic {}", action, lockName);
        inflight.compute(key, (topicName, waiters) -> {
            if (waiters == null) {
                LOGGER.debugCr(logContext.toReconciliation(), "Adding first waiter {}", action);
                return 1;
            } else {
                LOGGER.debugCr(logContext.toReconciliation(), "Adding waiter {}: {}", action, waiters + 1);
                return waiters + 1;
            }
        });
        vertx.sharedData().getLockWithTimeout(lockName, timeoutMs, lockResult -> {
            if (lockResult.succeeded()) {
                LOGGER.debugCr(logContext.toReconciliation(), "Lock acquired");
                LOGGER.debugCr(logContext.toReconciliation(), "Executing action {} on topic {}", action, lockName);
                action.execute().onComplete(actionResult -> {
                    LOGGER.debugCr(logContext.toReconciliation(), "Executing handler for action {} on topic {}", action, lockName);
                    action.result = actionResult;
                    String keytag = namespace + ":" + "KafkaTopic" + "/" + key.asKubeName().toString();
                    Optional<Meter> metric = metrics.meterRegistry().getMeters()
                            .stream()
                            .filter(meter -> meter.getId().getName().equals(METRICS_PREFIX + "resource.state") &&
                                    meter.getId().getTags().contains(Tag.of("kind", "KafkaTopic")) &&
                                    meter.getId().getTags().contains(Tag.of("name",  action.topic == null ? key.asKubeName().toString() : action.topic.getMetadata().getName())) &&
                                    meter.getId().getTags().contains(Tag.of("resource-namespace", namespace))
                            ).findFirst();
                    if (metric.isPresent()) {
                        // remove metric so it can be re-added with new tags
                        metrics.meterRegistry().remove(metric.get().getId());
                        LOGGER.debugCr(logContext.toReconciliation(), "Removed metric {}.resource.state{{}}", METRICS_PREFIX, keytag);
                    }

                    if (action.topic != null) {
                        boolean succeeded = actionResult.succeeded();
                        Tags metricTags;
                        metricTags = Tags.of(
                                Tag.of("kind", action.topic.getKind()),
                                Tag.of("name", action.topic.getMetadata().getName()),
                                Tag.of("resource-namespace", namespace),
                                Tag.of("reason", succeeded ? "none" : actionResult.cause().getMessage() == null ? "unknown error" : actionResult.cause().getMessage()));

                        metrics.gauge(METRICS_PREFIX + "resource.state", "Current state of the resource: 1 ready, 0 fail", metricTags).set(actionResult.succeeded() ? 1 : 0);
                        LOGGER.debugCr(logContext.toReconciliation(), "Updated metric " + METRICS_PREFIX + "resource.state{} = {}", metricTags, succeeded ? 1 : 0);
                    }
                    // Update status with lock held so that event is ignored via statusUpdateGeneration
                    action.updateStatus(logContext).onComplete(statusResult -> {
                        if (statusResult.failed()) {
                            LOGGER.errorCr(logContext.toReconciliation(), "Error updating KafkaTopic.status for action {}", action,
                                    statusResult.cause());
                        }
                        try {
                            if (actionResult.failed() && statusResult.failed()) {
                                actionResult.cause().addSuppressed(statusResult.cause());
                            }
                            result.handle(actionResult.failed() ? actionResult : statusResult);
                        } catch (Throwable t) {
                            result.fail(t);
                        } finally {
                            lockResult.result().release();
                            LOGGER.debugCr(logContext.toReconciliation(), "Lock released");
                            inflight.compute(key, decrement);
                        }
                    });
                });
            } else {
                lockedReconciliationsCounter.increment();
                LOGGER.warnCr(logContext.toReconciliation(), "Lock not acquired within {}ms: action {} will not be run", timeoutMs, action);
                try {
                    result.handle(Future.failedFuture("Failed to acquire lock for topic " + lockName + " after " + timeoutMs + "ms. Not executing action " + action));
                } finally {
                    inflight.compute(key, decrement);
                }
            }
        });
        return result.future();
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
    Future<Void> reconcile(Reconciliation reconciliation, final LogContext logContext, final HasMetadata involvedObject,
                   final Topic k8sTopic, final Topic kafkaTopic, final Topic privateTopic) {
        final Future<Void> reconciliationResultHandler;
        {
            TopicName topicName = k8sTopic != null ? k8sTopic.getTopicName() : kafkaTopic != null ? kafkaTopic.getTopicName() : privateTopic != null ? privateTopic.getTopicName() : null;
            LOGGER.infoCr(logContext.toReconciliation(), "Reconciling topic {}, k8sTopic:{}, kafkaTopic:{}, privateTopic:{}", topicName, k8sTopic == null ? "null" : "nonnull", kafkaTopic == null ? "null" : "nonnull", privateTopic == null ? "null" : "nonnull");
        }
        if (k8sTopic != null && Annotations.isReconciliationPausedWithAnnotation(k8sTopic.getMetadata())) {
            LOGGER.debugCr(logContext.toReconciliation(), "Reconciliation paused, not applying changes.");
            reconciliationResultHandler = Future.succeededFuture();
        } else if (privateTopic == null) {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // All three null: This happens reentrantly when a topic or KafkaTopic is deleted
                    LOGGER.debugCr(logContext.toReconciliation(), "All three topics null during reconciliation.");
                    reconciliationResultHandler = Future.succeededFuture();
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    LOGGER.debugCr(logContext.toReconciliation(), "topic created in kafka, will create KafkaTopic in k8s and topicStore");
                    reconciliationResultHandler = createResource(logContext, kafkaTopic)
                            .compose(createdKt -> {
                                reconciliation.observedTopicFuture(createdKt);
                                return createInTopicStore(logContext, kafkaTopic, involvedObject);
                            });
                }
            } else if (kafkaTopic == null) {
                // it's been created in k8s => create in Kafka and privateState
                LOGGER.debugCr(logContext.toReconciliation(), "KafkaTopic created in k8s, will create topic in kafka and topicStore");
                reconciliationResultHandler = createKafkaTopic(logContext, k8sTopic, involvedObject)
                    .compose(ignore -> createInTopicStore(logContext, k8sTopic, involvedObject))
                    // Kafka will set the message.format.version, so we need to update the KafkaTopic to reflect
                    // that to avoid triggering another reconciliation
                    .compose(ignored -> getFromKafka(logContext.toReconciliation(), k8sTopic.getTopicName()))
                    .compose(kafkaTopic2 -> {
                        LOGGER.debugCr(logContext.toReconciliation(), "Post-create kafka {}", kafkaTopic2);
                        if (kafkaTopic2 == null) {
                            LOGGER.errorCr(logContext.toReconciliation(), "Post-create kafka unexpectedly null");
                            return Future.succeededFuture();
                        }
                        return update3Way(reconciliation, logContext, involvedObject, k8sTopic, kafkaTopic2, k8sTopic);
                    });
                    //.compose(createdKafkaTopic -> update3Way(logContext, involvedObject, k8sTopic, createdKafkaTopic, k8sTopic));
            } else {
                reconciliationResultHandler = update2Way(reconciliation, logContext, involvedObject, k8sTopic, kafkaTopic);
            }
        } else {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // delete privateState
                    LOGGER.debugCr(logContext.toReconciliation(), "KafkaTopic deleted in k8s and topic deleted in kafka => delete from topicStore");
                    reconciliationResultHandler = deleteFromTopicStore(logContext, involvedObject, privateTopic.getTopicName());
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    // If delete.topic.enable=false then the resulting exception will be ignored and only the privateState topic will be deleted
                    LOGGER.debugCr(logContext.toReconciliation(), "KafkaTopic deleted in k8s => delete topic from kafka and from topicStore");
                    reconciliationResultHandler = deleteKafkaTopic(logContext, kafkaTopic.getTopicName()).recover(thrown -> handleTopicDeletionDisabled(thrown, logContext))
                        .compose(ignored -> deleteFromTopicStore(logContext, involvedObject, privateTopic.getTopicName()));
                }
            } else if (kafkaTopic == null) {
                // it was deleted in kafka so delete in k8s and privateState
                LOGGER.debugCr(logContext.toReconciliation(), "topic deleted in kafkas => delete KafkaTopic from k8s and from topicStore");
                reconciliationResultHandler = deleteResource(logContext, privateTopic.getOrAsKubeName())
                        .compose(ignore -> {
                            reconciliation.observedTopicFuture(null);
                            return deleteFromTopicStore(logContext, involvedObject, privateTopic.getTopicName());
                        });
            } else {
                // all three exist
                LOGGER.debugCr(logContext.toReconciliation(), "3 way diff");
                reconciliationResultHandler = update3Way(reconciliation, logContext, involvedObject,
                        k8sTopic, kafkaTopic, privateTopic);
            }
        }

        return reconciliationResultHandler.onComplete(res -> {
            if (res.succeeded()) {
                reconciliation.succeeded();
            } else {
                reconciliation.failed();
            }
        });
    }

    /**
     * Function for handling the exceptions thrown by attempting to delete a topic. If the  delete.topic.enable config
     * is set to false on the broker the exception is ignored an a blank future returned. For any other form of exception
     * a failed future is returned using that exception as the cause.
     *
     * @param thrown The exception encountered when attempting to delete the kafka topic.
     * @return Either an succeeded future in the case that topic deletion is disabled or a failed future in all other cases.
     */
    private Future<Void> handleTopicDeletionDisabled(Throwable thrown, LogContext logContext) {

        if (thrown instanceof org.apache.kafka.common.errors.TopicDeletionDisabledException) {
            LOGGER.warnCr(logContext.toReconciliation(), "Topic deletion is disabled. Kafka topic will persist and KafkaTopic resource will be recreated in the next reconciliation.");
        } else {
            LOGGER.errorCr(logContext.toReconciliation(), "Topic deletion failed with ({}) error: {}", thrown.getClass(), thrown.getMessage());
            return Future.failedFuture(thrown);
        }

        return Future.succeededFuture();
    }

    private Future<Void> update2Way(Reconciliation reconciliation, LogContext logContext, HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic) {
        final Future<Void> reconciliationResultHandler;
        TopicDiff diff = TopicDiff.diff(kafkaTopic, k8sTopic);
        if (diff.isEmpty()) {
            // they're the same => do nothing, but still create the private copy
            LOGGER.debugCr(logContext.toReconciliation(), "KafkaTopic created in k8s and topic created in kafka, but they're identical => just creating in topicStore");
            LOGGER.debugCr(logContext.toReconciliation(), "k8s and kafka versions of topic '{}' are the same", kafkaTopic.getTopicName());
            Topic privateTopic = new Topic.Builder(kafkaTopic)
                    .withMapName(k8sTopic.getResourceName().toString())
                    .build();
            reconciliationResultHandler = createInTopicStore(logContext, privateTopic, involvedObject);
        } else if (!diff.changesReplicationFactor()
                && !diff.changesNumPartitions()
                && diff.changesConfig()
                && disjoint(kafkaTopic.getConfig().keySet(), k8sTopic.getConfig().keySet())) {
            LOGGER.debugCr(logContext.toReconciliation(), "KafkaTopic created in k8s and topic created in kafka, they differ only in topic config, and those configs are disjoint: Updating k8s and kafka, and creating in topic store");
            Map<String, String> mergedConfigs = new HashMap<>(kafkaTopic.getConfig());
            mergedConfigs.putAll(k8sTopic.getConfig());
            Topic mergedTopic = new Topic.Builder(kafkaTopic)
                    .withConfig(mergedConfigs)
                    .withMapName(k8sTopic.getResourceName().toString())
                    .build();
            reconciliationResultHandler = updateResource(logContext, mergedTopic)
                    .compose(updatedResource -> {
                        reconciliation.observedTopicFuture(updatedResource);
                        Promise<Void> x = Promise.promise();
                        enqueue(logContext, new UpdateKafkaConfig(logContext, mergedTopic, involvedObject, x));
                        return x.future().compose(ignore -> createInTopicStore(logContext, mergedTopic, involvedObject));
                    });
        } else {
            // Just use kafka version, but also create a warning event
            LOGGER.debugCr(logContext.toReconciliation(), "KafkaTopic created in k8s and topic created in kafka, and they are irreconcilably different => kafka version wins");
            Promise<Void> eventPromise = Promise.promise();
            enqueue(logContext, new Event(logContext, involvedObject, "KafkaTopic is incompatible with the topic metadata. " +
                    "The topic metadata will be treated as canonical.", EventType.INFO, eventPromise));
            reconciliationResultHandler = eventPromise.future()
                .compose(ignored ->
                    updateResource(logContext, kafkaTopic))
                .compose(updatedResource -> {
                    reconciliation.observedTopicFuture(updatedResource);
                    Topic privateTopic = new Topic.Builder(kafkaTopic)
                            .withMapName(k8sTopic.getResourceName().toString())
                            .build();
                    return createInTopicStore(logContext, privateTopic, involvedObject);
                });
        }
        return reconciliationResultHandler;
    }

    private Future<Void> update3Way(Reconciliation reconciliation, LogContext logContext, HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Topic privateTopic) {
        final Future<Void> reconciliationResultHandler;
        if (!privateTopic.getResourceName().equals(k8sTopic.getResourceName())) {
            return Future.failedFuture(new OperatorException(involvedObject,
                    "Topic '" + kafkaTopic.getTopicName() + "' is already managed via KafkaTopic '" + privateTopic.getResourceName() + "' it cannot also be managed via the KafkaTopic '" + k8sTopic.getResourceName() + "'"));
        }
        TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
        LOGGER.debugCr(logContext.toReconciliation(), "topicStore->kafkaTopic: {}", oursKafka);
        TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
        LOGGER.debugCr(logContext.toReconciliation(), "topicStore->k8sTopic: {}", oursK8s);
        String conflict = oursKafka.conflict(oursK8s);
        if (conflict != null) {
            final String message = "KafkaTopic resource and Kafka topic both changed in a conflicting way: " + conflict;
            LOGGER.errorCr(logContext.toReconciliation(), "{}", message);
            enqueue(logContext, new Event(logContext, involvedObject, message, EventType.INFO, eventResult -> { }));
            reconciliationResultHandler = Future.failedFuture(new ConflictingChangesException(involvedObject, message));
        } else {
            TopicDiff merged = oursKafka.merge(oursK8s);
            LOGGER.debugCr(logContext.toReconciliation(), "Diffs do not conflict, merged diff: {}", merged);
            if (merged.isEmpty()) {
                LOGGER.infoCr(logContext.toReconciliation(), "All three topics are identical");
                reconciliationResultHandler = Future.succeededFuture();
            } else {
                Topic result = merged.apply(privateTopic);
                int partitionsDelta = merged.numPartitionsDelta();
                if (partitionsDelta < 0) {
                    final String message = "Number of partitions cannot be decreased";
                    LOGGER.errorCr(logContext.toReconciliation(), "{}", message);
                    enqueue(logContext, new Event(logContext, involvedObject, message, EventType.INFO, eventResult -> {
                    }));
                    reconciliationResultHandler = Future.failedFuture(new PartitionDecreaseException(involvedObject, message));
                } else if (oursK8s.changesReplicationFactor()
                            && !oursKafka.changesReplicationFactor()) {
                    reconciliationResultHandler = Future.failedFuture(new ReplicationFactorChangeException(involvedObject,
                                    "Changing 'spec.replicas' is not supported. " +
                                            "This KafkaTopic's 'spec.replicas' should be reverted to " +
                                            kafkaTopic.getNumReplicas() +
                                            " and then the replication should be changed directly in Kafka."));
                } else {
                    // TODO What if we increase min.in.sync.replicas and the number of replicas,
                    // such that the old number of replicas < the new min isr? But likewise
                    // we could decrease, so order of tasks in the queue will need to change
                    // depending on what the diffs are.
                    LOGGER.debugCr(logContext.toReconciliation(), "Updating KafkaTopic, kafka topic and topicStore");
                    TopicDiff kubeDiff = TopicDiff.diff(k8sTopic, result);
                    reconciliationResultHandler = Future.succeededFuture()
                        .compose(updatedKafkaTopic -> {
                            Future<Void> configFuture;
                            TopicDiff kafkaDiff = TopicDiff.diff(kafkaTopic, result);
                            if (merged.changesConfig()
                                    && !kafkaDiff.isEmpty()) {
                                Promise<Void> promise = Promise.promise();
                                configFuture = promise.future();
                                LOGGER.debugCr(logContext.toReconciliation(), "Updating kafka config with {}", kafkaDiff);
                                enqueue(logContext, new UpdateKafkaConfig(logContext, result, involvedObject, promise));
                            } else {
                                LOGGER.debugCr(logContext.toReconciliation(), "No need to update kafka topic with {}", kafkaDiff);
                                configFuture = Future.succeededFuture();
                            }
                            return configFuture;
                        }).compose(ignored -> {
                            Future<KafkaTopic> resourceFuture;
                            if (!kubeDiff.isEmpty()) {
                                LOGGER.debugCr(logContext.toReconciliation(), "Updating KafkaTopic with {}", kubeDiff);
                                resourceFuture = updateResource(logContext, result).map(updatedKafkaTopic -> {
                                    reconciliation.observedTopicFuture(updatedKafkaTopic);
                                    return updatedKafkaTopic;
                                });
                            } else {
                                LOGGER.debugCr(logContext.toReconciliation(), "No need to update KafkaTopic {}", kubeDiff);
                                resourceFuture = Future.succeededFuture();
                            }
                            return resourceFuture;
                        })
                        .compose(ignored -> {
                            if (partitionsDelta > 0
                                    // Kafka throws an error if we attempt a noop change #partitions
                                    && result.getNumPartitions() > kafkaTopic.getNumPartitions()) {
                                Promise<Void> partitionsPromise = Promise.promise();
                                enqueue(logContext, new IncreaseKafkaPartitions(logContext, result, involvedObject, partitionsPromise));
                                return partitionsPromise.future();
                            } else {
                                return Future.succeededFuture();
                            }
                        }).compose(ignored -> {
                            Promise<Void> topicStorePromise = Promise.promise();
                            enqueue(logContext, new UpdateInTopicStore(logContext, result, involvedObject, topicStorePromise));
                            return topicStorePromise.future();
                        });
                }
            }
        }
        return reconciliationResultHandler;
    }

    void enqueue(LogContext logContext, Handler<Void> event) {
        LOGGER.debugCr(logContext.toReconciliation(), "Enqueuing event {}", event);
        vertx.runOnContext(event);
    }

    /** Called when a topic znode is deleted in ZK */
    Future<Void> onTopicDeleted(LogContext logContext, TopicName topicName) {
        Future<Void> confirmedNonexistence = awaitExistential(logContext, topicName, false);
        return confirmedNonexistence
        .compose(
            ignored ->
                executeWithTopicLockHeld(logContext, topicName,
                    new Reconciliation(logContext, "onTopicDeleted", true) {
                        @Override
                        public Future<Void> execute() {
                            return reconcileOnTopicChange(logContext, topicName, null, this);
                        }
                    }),
            error ->
                Future.failedFuture("Ignored spurious-seeming topic deletion"));
    }

    private Future<Void> awaitExistential(LogContext logContext, TopicName topicName, boolean checkExists) {
        String logState = "confirmed " + (checkExists ? "" : "non-") + "existence";
        AtomicReference<Future<Boolean>> ref = new AtomicReference<>(kafka.topicExists(logContext.toReconciliation(), topicName));
        return Util.waitFor(logContext.toReconciliation(), vertx, logContext.toString(), logState, 1_000, 60_000,
            () -> {
                Future<Boolean> existsFuture = ref.get();
                if (existsFuture.isComplete()) {
                    if ((!checkExists && !existsFuture.result())
                            || (checkExists && existsFuture.result())) {
                        return true;
                    } else {
                        // It still exists (or still doesn't exist), so ask again, until we timeout
                        ref.set(kafka.topicExists(logContext.toReconciliation(), topicName));
                        return false;
                    }
                }
                return false;
            });
    }

    /**
     * Called when ZK watch notifies of change to topic's config
     */
    Future<Void> onTopicConfigChanged(LogContext logContext, TopicName topicName) {
        return executeWithTopicLockHeld(logContext, topicName,
                new Reconciliation(logContext, "onTopicConfigChanged", true) {
                    @Override
                    public Future<Void> execute() {
                        return kafka.topicMetadata(logContext.toReconciliation(), topicName)
                                .compose(metadata -> {
                                    Topic topic = TopicSerialization.fromTopicMetadata(metadata);
                                    return reconcileOnTopicChange(logContext, topicName, topic, this);
                                });
                    }
                });
    }

    /**
     * Called when ZK watch notifies of a change to the topic's partitions
     */
    Future<Void> onTopicPartitionsChanged(LogContext logContext, TopicName topicName) {
        Reconciliation action = new Reconciliation(logContext, "onTopicPartitionsChanged", true) {
            @Override
            public Future<Void> execute() {
                Reconciliation self = this;
                Promise<Void> promise = Promise.promise();
                // getting topic information from the private store
                topicStore.read(topicName).onComplete(topicResult -> {

                    TopicMetadataHandler handler = new TopicMetadataHandler(vertx, kafka, topicName, topicMetadataBackOff()) {
                        @Override
                        public void handle(AsyncResult<TopicMetadata> metadataResult) {
                            try {
                                if (metadataResult.succeeded()) {
                                    // getting topic metadata from Kafka
                                    Topic kafkaTopic = TopicSerialization.fromTopicMetadata(metadataResult.result());

                                    // if partitions aren't changed on Kafka yet, we retry with exponential backoff
                                    if (topicResult.result().getNumPartitions() == kafkaTopic.getNumPartitions()) {
                                        retry(logContext.toReconciliation());
                                    } else {
                                        LOGGER.infoCr(logContext.toReconciliation(), "Topic {} partitions changed to {}", topicName, kafkaTopic.getNumPartitions());
                                        reconcileOnTopicChange(logContext, topicName, kafkaTopic, self)
                                            .onComplete(promise);
                                    }

                                } else {
                                    promise.fail(metadataResult.cause());
                                }
                            } catch (Throwable t) {
                                promise.fail(t);
                            }
                        }

                        @Override
                        public void onMaxAttemptsExceeded(MaxAttemptsExceededException e) {
                            // it's possible that the watched znode for partitions changes, is changed
                            // due to a reassignment if we don't observe a partition count change within the backoff
                            // no need for failing the future in this case
                            promise.complete();
                        }
                    };
                    kafka.topicMetadata(logContext.toReconciliation(), topicName).onComplete(handler);
                });
                return promise.future();
            }
        };
        return executeWithTopicLockHeld(logContext, topicName, action);
    }

    /**
     * Called when one of the ZK watches notifies of a change to the topic
     */
    private Future<Void> reconcileOnTopicChange(LogContext logContext, TopicName topicName, Topic kafkaTopic,
                                                Reconciliation reconciliation) {
        // Look up the private topic to discover the name of kube KafkaTopic
        return topicStore.read(topicName)
            .compose(storeTopic -> {
                ResourceName resourceName = storeTopic != null ? storeTopic.getResourceName() : topicName.asKubeName();
                return k8s.getFromName(resourceName).compose(topic -> {
                    reconciliation.observedTopicFuture(kafkaTopic != null ? topic : null);
                    Topic k8sTopic = TopicSerialization.fromTopicResource(topic);
                    return reconcile(reconciliation, logContext.withKubeTopic(topic), topic, k8sTopic, kafkaTopic, storeTopic);
                });
            });
    }

    /** Called when a topic znode is created in ZK */
    Future<Void> onTopicCreated(LogContext logContext, TopicName topicName) {
        // XXX currently runs on the ZK thread, requiring a synchronized inFlight
        // is it better to put this check in the topic deleted event?
        Reconciliation action = new Reconciliation(logContext, "onTopicCreated", true) {
            @Override
            public Future<Void> execute() {
                Reconciliation self = this;
                Promise<Void> promise = Promise.promise();
                TopicMetadataHandler handler = new TopicMetadataHandler(vertx, kafka, topicName, topicMetadataBackOff()) {

                    @Override
                    public void handle(AsyncResult<TopicMetadata> metadataResult) {

                        if (metadataResult.succeeded()) {
                            if (metadataResult.result() == null) {
                                // In this case it is most likely that we've been notified by ZK
                                // before Kafka has finished creating the topic, so we retry
                                // with exponential backoff.
                                retry(logContext.toReconciliation());
                            } else {
                                // We now have the metadata we need to create the
                                // resource...
                                Topic kafkaTopic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                                reconcileOnTopicChange(logContext, topicName, kafkaTopic, self)
                                        .onComplete(promise);
                            }
                        } else {
                            promise.fail(metadataResult.cause());
                        }
                    }

                    @Override
                    public void onMaxAttemptsExceeded(MaxAttemptsExceededException e) {
                        promise.fail(e);
                    }
                };
                return awaitExistential(logContext, topicName, true).compose(exists ->  {
                    kafka.topicMetadata(logContext.toReconciliation(), topicName).onComplete(handler);
                    return promise.future();
                });
            }
        };
        return executeWithTopicLockHeld(logContext, topicName, action);
    }

    abstract class Reconciliation {
        private final LogContext logContext;
        private final String name;
        private final boolean watchedForMetrics;
        public AsyncResult<Void> result;
        public volatile KafkaTopic topic;
        Timer.Sample reconciliationTimerSample;

        public Reconciliation(LogContext logContext, String name, boolean watchedForMetrics) {
            this.logContext = logContext;
            this.watchedForMetrics = watchedForMetrics;
            this.name = name;
            if (isEventWatched()) {
                LOGGER.debugCr(logContext.toReconciliation(), "Metric {} triggered", this.name);
                this.reconciliationTimerSample = Timer.start(metrics.meterRegistry());
                reconciliationsCounter.increment();
            }
        }

        public void failed() {
            if (isEventWatched()) {
                LOGGER.debugCr(logContext.toReconciliation(), "failed reconciliation {}", name);
                reconciliationTimerSample.stop(reconciliationsTimer);
                failedReconciliationsCounter.increment();
            }
        }

        public void succeeded() {
            if (isEventWatched()) {
                LOGGER.debugCr(logContext.toReconciliation(), "succeeded reconciliation {}", name);
                reconciliationTimerSample.stop(reconciliationsTimer);
                successfulReconciliationsCounter.increment();
            }
        }

        private boolean isEventWatched() {
            return watchedForMetrics;
        }

        @Override
        public String toString() {
            return name;
        }

        public abstract Future<Void> execute();

        protected void observedTopicFuture(KafkaTopic observedTopic) {
            topic = observedTopic;
        }

        private Future<Void> updateStatus(LogContext logContext) {
            try {
                KafkaTopic topic = this.topic;
                Future<Void> statusFuture;
                if (topic != null) {
                    // Get the existing status and if it's og == g and is has same status then don't update
                    LOGGER.debugCr(logContext.toReconciliation(), "There is a KafkaTopic to set status on, rv={}, generation={}",
                            topic.getMetadata().getResourceVersion(),
                            topic.getMetadata().getGeneration());
                    KafkaTopicStatus kts = new KafkaTopicStatus();
                    StatusUtils.setStatusConditionAndObservedGeneration(topic, kts, result);

                    if (topic.getStatus() == null || topic.getStatus().getTopicName() == null) {
                        String specTopicName = new TopicName(topic).toString();
                        kts.setTopicName(specTopicName);
                    } else {
                        kts.setTopicName(topic.getStatus().getTopicName());
                    }

                    if (Annotations.isReconciliationPausedWithAnnotation(topic)) {
                        kts.setConditions(singletonList(StatusUtils.getPausedCondition()));
                    }

                    StatusDiff ksDiff = new StatusDiff(topic.getStatus(), kts);
                    if (!ksDiff.isEmpty()) {
                        Promise<Void> promise = Promise.promise();
                        statusFuture = promise.future();
                        k8s.updateResourceStatus(logContext.toReconciliation(), new KafkaTopicBuilder(topic).withStatus(kts).build()).onComplete(ar -> {
                            if (ar.succeeded() && ar.result() != null) {
                                ObjectMeta metadata = ar.result().getMetadata();
                                LOGGER.debugCr(logContext.toReconciliation(), "status was set rv={}, generation={}, observedGeneration={}",
                                        metadata.getResourceVersion(),
                                        metadata.getGeneration(),
                                        ar.result().getStatus().getObservedGeneration());
                            } else {
                                LOGGER.errorCr(logContext.toReconciliation(), "Error setting resource status", ar.cause());
                            }
                            promise.handle(ar.map((Void) null));
                        });
                    } else {
                        statusFuture = Future.succeededFuture();
                    }
                } else {
                    LOGGER.debugCr(logContext.toReconciliation(), "No KafkaTopic to set status");
                    statusFuture = Future.succeededFuture();
                }
                return statusFuture;
            } catch (Throwable t) {
                LOGGER.errorCr(logContext.toReconciliation(), "{}", t);
                return Future.failedFuture(t);
            }
        }
    }

    /** Called when a resource is isModify in k8s */
    Future<Void> onResourceEvent(LogContext logContext, KafkaTopic modifiedTopic, Watcher.Action action) {
        return executeWithTopicLockHeld(logContext, new TopicName(modifiedTopic),
                new Reconciliation(logContext, "onResourceEvent", false) {
                    @Override
                    public Future<Void> execute() {
                        return k8s.getFromName(new ResourceName(modifiedTopic))
                            .compose(mt ->  {
                                final Topic k8sTopic;
                                if (mt != null) {
                                    observedTopicFuture(mt);
                                    try {
                                        k8sTopic = TopicSerialization.fromTopicResource(mt);
                                    } catch (InvalidTopicException e) {
                                        return Future.failedFuture(e);
                                    }
                                } else {
                                    k8sTopic = null;
                                }
                                return reconcileOnResourceChange(this, logContext, mt != null ? mt : modifiedTopic, k8sTopic, action == Watcher.Action.MODIFIED);
                            });
                    }
                });
    }

    private Future<Void> checkForNameChange(TopicName topicName, KafkaTopic topicResource) {
        if (topicResource == null) {
            return Future.succeededFuture();
        }
        TopicName topicNameFromStatus = topicResource.getStatus() == null ? null : new TopicName(topicResource.getStatus().getTopicName());
        if (topicNameFromStatus != null && !topicName.equals(topicNameFromStatus)) {
            return Future.failedFuture(new IllegalArgumentException("Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed."));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> reconcileOnResourceChange(Reconciliation reconciliation, LogContext logContext, KafkaTopic topicResource, Topic k8sTopic,
                                           boolean isModify) {
        TopicName topicName = new TopicName(topicResource);

        return checkForNameChange(topicName, topicResource)
            .onComplete(nameChanged -> {
                if (nameChanged.failed()) {
                    enqueue(logContext, new Event(logContext, topicResource,
                            "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.",
                            EventType.WARNING, eventResult -> { }));
                }
            })
            .compose(i -> CompositeFuture.all(getFromKafka(logContext.toReconciliation(), topicName), getFromTopicStore(topicName))
                .compose(compositeResult -> {
                    Topic kafkaTopic = compositeResult.resultAt(0);
                    Topic privateTopic = compositeResult.resultAt(1);
                    if (kafkaTopic == null
                            && privateTopic == null
                            && isModify
                            && topicResource.getMetadata().getDeletionTimestamp() != null) {
                        // When processing a Kafka-side deletion then when we delete the KT
                        // We first receive a modify event (setting deletionTimestamp etc)
                        // then the deleted event. We need to ignore the modify event.
                        LOGGER.debugCr(logContext.toReconciliation(), "Ignoring pre-delete modify event");
                        reconciliation.observedTopicFuture(null);
                        return Future.succeededFuture();
                    } else {
                        return reconcile(reconciliation, logContext, topicResource, k8sTopic, kafkaTopic, privateTopic);
                    }
                }));
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
            topicStore.update(topic).onComplete(ar -> {
                if (ar.failed()) {
                    enqueue(logContext, new Event(logContext, involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "UpdateInTopicStore(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }

    private Future<Void> createInTopicStore(LogContext logContext, Topic topic, HasMetadata involvedObject) {
        Promise<Void> result = Promise.promise();
        enqueue(logContext, new CreateInTopicStore(logContext, topic, involvedObject, result));
        return result.future();
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
            LOGGER.debugCr(logContext.toReconciliation(), "Executing {}", this);
            topicStore.create(topic).onComplete(ar -> {
                LOGGER.debugCr(logContext.toReconciliation(), "Completing {}", this);
                if (ar.failed()) {
                    LOGGER.debugCr(logContext.toReconciliation(), "{} failed", this);
                    enqueue(logContext, new Event(logContext, involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
                } else {
                    LOGGER.debugCr(logContext.toReconciliation(), "{} succeeded", this);
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "CreateInTopicStore(topicName=" + topic.getTopicName() + ",ctx=" + logContext + ")";
        }
    }


    private Future<Void> deleteFromTopicStore(LogContext logContext, HasMetadata involvedObject, TopicName topicName) {
        Promise<Void> reconciliationResultHandler = Promise.promise();
        enqueue(logContext, new DeleteFromTopicStore(logContext, topicName, involvedObject, reconciliationResultHandler));
        return reconciliationResultHandler.future();
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
            topicStore.delete(topicName).onComplete(ar -> {
                if (ar.failed()) {
                    enqueue(logContext, new Event(logContext, involvedObject, ar.cause().toString(), EventType.WARNING, eventResult -> { }));
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
        LOGGER.debugOp("Outstanding: {}", inflight);
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
            this.ktList = emptyList();
        }

        public void setKafkaTopics(List<KafkaTopic> ktList) {
            this.ktList = ktList;
        }
    }

    Future<?> reconcileAllTopics(String reconciliationType) {
        LOGGER.infoOp("Starting {} reconciliation", reconciliationType);
        return kafka.listTopics().recover(ex -> Future.failedFuture(
                new OperatorException("Error listing existing topics during " + reconciliationType + " reconciliation", ex)
        )).compose(topicNamesFromKafka ->
                // Reconcile the topic found in Kafka
                reconcileFromKafka(reconciliationType, topicNamesFromKafka.stream().map(TopicName::new).collect(Collectors.toList()))
        ).compose(reconcileState -> {
            Future<List<KafkaTopic>> ktFut = k8s.listResources();
            return ktFut.recover(ex -> Future.failedFuture(
                    new OperatorException("Error listing existing KafkaTopics during " + reconciliationType + " reconciliation", ex)
            )).map(ktList -> {
                reconcileState.setKafkaTopics(ktList);
                return reconcileState;
            });
        }).compose(reconcileState -> {
            List<Future> futs = new ArrayList<>();
            pausedTopicCounter.set(0);
            topicCounter.set(reconcileState.ktList.size());
            for (KafkaTopic kt : reconcileState.ktList) {
                if (Annotations.isReconciliationPausedWithAnnotation(kt)) {
                    pausedTopicCounter.getAndIncrement();
                }
                LogContext logContext = LogContext.periodic(reconciliationType + "kube " + kt.getMetadata().getName(), kt.getMetadata().getNamespace(), kt.getMetadata().getName()).withKubeTopic(kt);
                Topic topic = TopicSerialization.fromTopicResource(kt);
                TopicName topicName = topic.getTopicName();
                if (reconcileState.failed.containsKey(topicName)) {
                    // we already failed to reconcile this topic in reconcileFromKafka(), /
                    // don't bother trying again
                    LOGGER.traceCr(logContext.toReconciliation(), "Already failed to reconcile {}", topicName);
                    reconciliationsCounter.increment();
                    failedReconciliationsCounter.increment();
                } else if (reconcileState.succeeded.contains(topicName)) {
                    // we already succeeded in reconciling this topic in reconcileFromKafka()
                    LOGGER.traceCr(logContext.toReconciliation(), "Already successfully reconciled {}", topicName);
                    reconciliationsCounter.increment();
                    successfulReconciliationsCounter.increment();
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
                    LOGGER.debugCr(logContext.toReconciliation(), "Topic {} exists in Kubernetes, but not Kafka", topicName, logTopic(kt));
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
                    LogContext logContext = LogContext.periodic(reconciliationType + "-" + tn, namespace, tn.asKubeName().toString());
                    futs2.add(executeWithTopicLockHeld(logContext, tn, new Reconciliation(logContext, "delete-remaining", true) {
                        @Override
                        public Future<Void> execute() {
                            observedTopicFuture(null);
                            return getKafkaAndReconcile(this, logContext, tn, null, null);
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

        LOGGER.debugOp("Reconciling kafka topics {}", topicsFromKafka);

        final ReconcileState state = new ReconcileState(succeeded, undetermined, failed);
        if (topicsFromKafka.size() > 0) {
            List<Future<Void>> futures = new ArrayList<>();
            for (TopicName topicName : topicsFromKafka) {
                LogContext logContext = LogContext.periodic(reconciliationType + "kafka " + topicName, namespace, topicName.asKubeName().toString());
                futures.add(executeWithTopicLockHeld(logContext, topicName, new Reconciliation(logContext, "reconcile-from-kafka", false) {
                    @Override
                    public Future<Void> execute() {
                        return getFromTopicStore(topicName).recover(error -> {
                            failed.put(topicName,
                                    new OperatorException("Error getting topic " + topicName + " from topic store during "
                                            + reconciliationType + " reconciliation", error));
                            return Future.succeededFuture();
                        }).compose(topic -> {
                            if (topic == null) {
                                LOGGER.debugCr(logContext.toReconciliation(), "No private topic for topic {} in Kafka -> undetermined", topicName);
                                undetermined.add(topicName);
                                return Future.succeededFuture();
                            } else {
                                LOGGER.debugCr(logContext.toReconciliation(), "Have private topic for topic {} in Kafka", topicName);
                                return reconcileWithPrivateTopic(logContext, topicName, topic, this)
                                        .<Void>map(ignored -> {
                                            LOGGER.debugCr(logContext.toReconciliation(), "{} reconcile success -> succeeded", topicName);
                                            succeeded.add(topicName);
                                            return null;
                                        }).recover(error -> {
                                            LOGGER.debugCr(logContext.toReconciliation(), "{} reconcile error -> failed", topicName);
                                            failed.put(topicName, error);
                                            return Future.failedFuture(error);
                                        });
                            }
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
    private Future<Void> reconcileWithPrivateTopic(LogContext logContext, TopicName topicName,
                                                   Topic privateTopic,
                                                   Reconciliation reconciliation) {
        return k8s.getFromName(privateTopic.getResourceName())
            .recover(error -> {
                LOGGER.errorCr(logContext.toReconciliation(), "Error getting KafkaTopic {} for topic {}",
                        topicName.asKubeName(), topicName, error);
                return Future.failedFuture(new OperatorException("Error getting KafkaTopic " + topicName.asKubeName() + " during " + logContext.trigger() + " reconciliation", error));
            })
            .compose(kafkaTopicResource -> {
                reconciliation.observedTopicFuture(kafkaTopicResource);
                return getKafkaAndReconcile(reconciliation, logContext, topicName, privateTopic, kafkaTopicResource);
            });
    }

    private Future<Void> getKafkaAndReconcile(Reconciliation reconciliation, LogContext logContext, TopicName topicName,
                                              Topic privateTopic, KafkaTopic kafkaTopicResource) {
        logContext.withKubeTopic(kafkaTopicResource);
        Promise<Void> topicPromise = Promise.promise();
        try {
            Topic k8sTopic = kafkaTopicResource != null ? TopicSerialization.fromTopicResource(kafkaTopicResource) : null;
            checkForNameChange(topicName, kafkaTopicResource)
                .onComplete(nameChanged -> {
                    if (nameChanged.failed()) {
                        enqueue(logContext, new Event(logContext, kafkaTopicResource,
                                "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.",
                                EventType.WARNING, eventResult -> { }));
                    }
                })
                .compose(i -> kafka.topicMetadata(logContext.toReconciliation(), topicName))
                .compose(kafkaTopicMeta -> {
                    Topic topicFromKafka = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
                    return reconcile(reconciliation, logContext, kafkaTopicResource, k8sTopic, topicFromKafka, privateTopic);
                })
                .onComplete(ar -> {
                    if (ar.failed()) {
                        reconciliation.failed();
                        LOGGER.errorCr(logContext.toReconciliation(), "Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), ar.cause());
                    } else {
                        reconciliation.succeeded();
                        LOGGER.infoCr(logContext.toReconciliation(), "Success reconciling KafkaTopic {}", logTopic(kafkaTopicResource));
                    }
                    topicPromise.handle(ar);
                });
        } catch (InvalidTopicException e) {
            reconciliation.failed();
            LOGGER.errorCr(logContext.toReconciliation(), "Error reconciling KafkaTopic {}: Invalid resource: ", logTopic(kafkaTopicResource), e.getMessage());
            topicPromise.fail(e);
        } catch (OperatorException e) {
            reconciliation.failed();
            LOGGER.errorCr(logContext.toReconciliation(), "Error reconciling KafkaTopic {}", logTopic(kafkaTopicResource), e);
            topicPromise.fail(e);
        }
        return topicPromise.future();
    }

    Future<Topic> getFromKafka(io.strimzi.operator.common.Reconciliation reconciliation, TopicName topicName) {
        return kafka.topicMetadata(reconciliation, topicName).map(TopicSerialization::fromTopicMetadata);
    }

    Future<Topic> getFromTopicStore(TopicName topicName) {
        return topicStore.read(topicName);
    }

    private Future<Void> reconcileWithKubeTopic(LogContext logContext, HasMetadata involvedObject,
                                                String reconciliationType, ResourceName kubeName, TopicName topicName) {
        return executeWithTopicLockHeld(logContext, topicName, new Reconciliation(logContext, "reconcile-with-kube", true) {
            @Override
            public Future<Void> execute() {
                Reconciliation self = this;
                return CompositeFuture.all(
                        k8s.getFromName(kubeName).map(kt -> {
                            observedTopicFuture(kt);
                            return kt;
                        }),
                        getFromKafka(logContext.toReconciliation(), topicName),
                        getFromTopicStore(topicName))
                    .compose(compositeResult -> {
                        KafkaTopic ktr = compositeResult.resultAt(0);
                        logContext.withKubeTopic(ktr);
                        Topic k8sTopic = TopicSerialization.fromTopicResource(ktr);
                        Topic kafkaTopic = compositeResult.resultAt(1);
                        Topic privateTopic = compositeResult.resultAt(2);
                        return reconcile(self, logContext, involvedObject, k8sTopic, kafkaTopic, privateTopic);
                    });
            }
        });
    }

    protected String getNamespace() {
        return this.namespace;
    }

}

