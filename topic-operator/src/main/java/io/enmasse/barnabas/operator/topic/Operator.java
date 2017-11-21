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
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;


public class Operator {

    private final static Logger logger = LoggerFactory.getLogger(Operator.class);
    private final Kafka kafka;
    private final K8s k8s;
    private final KubernetesClient client;
    //private final ScheduledExecutorService executor;
    private final Vertx vertx;
    private final CmPredicate cmPredicate;
    private TopicStore topicStore;

    private final InFlight inFlight = new InFlight();

    static abstract class OperatorEvent implements Runnable, Handler<Void> {

        public abstract void process() throws OperatorException;

        public void run() {
            logger.info("Processing event {}", this);
            this.process();
            logger.info("Event {} processed successfully", this);
        }

        public void handle(Void v) {
            run();
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
            k8s.createEvent(event, ar -> {});
        }

        public String toString() {
            return "ErrorEvent(involvedObject="+involvedObject+", message="+message+")";
        }
    }


    /** Topic created in ZK */
    public class CreateConfigMap extends OperatorEvent {
        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public CreateConfigMap(Topic topic, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
        }

        @Override
        public void process() throws OperatorException {
            ConfigMap cm = TopicSerialization.toConfigMap(topic, cmPredicate);
            // TODO assert no existing mapping
            inFlight.startCreatingConfigMap(cm);
            k8s.createConfigMap(cm, handler);
        }

        @Override
        public String toString() {
            return "CreateConfigMap(topicName="+topic.getTopicName()+")";
        }
    }

    /** Topic deleted in ZK */
    public class DeleteConfigMap extends OperatorEvent {

        private final TopicName topicName;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public DeleteConfigMap(TopicName topicName, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.handler = handler;
        }

        @Override
        public void process() {
            // TODO assert no existing mapping
            inFlight.startDeletingConfigMap(topicName);
            k8s.deleteConfigMap(topicName, handler);
        }

        @Override
        public String toString() {
            return "DeleteConfigMap(topicName="+topicName+")";
        }
    }

    /** Topic config modified in ZK */
    public class UpdateConfigMap extends OperatorEvent {

        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;
        private final HasMetadata involvedObject;

        public UpdateConfigMap(Topic topic, Handler<io.vertx.core.AsyncResult<Void>> handler, HasMetadata involvedObject) {
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() {
            // TODO get topic data from AdminClient
            // How do we avoid a race here, where the topic exists in ZK, but not yet visible from AC?
            // Record that it's us who is creating to the config map
            // create ConfigMap in k8s
            // ignore the watch for the configmap creation
            ConfigMap cm = TopicSerialization.toConfigMap(topic, cmPredicate);
            // TODO assert no existing mapping
            inFlight.startUpdatingConfigMap(cm);
            k8s.updateConfigMap(cm, handler);
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
        private final Handler<AsyncResult<Void>> handler;

        public CreateKafkaTopic(Topic topic, Handler<AsyncResult<Void>> handler, HasMetadata involvedObject) {
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            inFlight.startCreatingTopic(topic.getTopicName());
            kafka.createTopic(TopicSerialization.toNewTopic(topic), ar -> {
                if (ar.succeeded()) {
                    logger.info("Created topic '{}' for ConfigMap '{}'", topic.getTopicName(), topic.getMapName());
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
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString()));
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
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString()));
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
        private final Handler<AsyncResult<Void>> handler;

        public DeleteKafkaTopic(TopicName topicName, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void process() throws OperatorException {
            logger.info("Deleting topic '{}'", topicName);
            // TODO assert no existing mapping
            inFlight.startDeletingTopic(topicName);
            kafka.deleteTopic(topicName, (result) -> {
                if (result.succeeded()) {
                    logger.info("Deleted topic '{}' for ConfigMap", topicName);
                    handler.handle(result);
                } else {
                    throw new OperatorException(involvedObject, result.cause());
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

    public Operator(DefaultKubernetesClient kubeClient, Kafka kafka,
                    K8s k8s, Vertx vertx,
                    CmPredicate cmPredicate) {
        this.kafka = kafka;
        this.client = kubeClient;
        this.k8s = k8s;
        this.vertx = vertx;
        this.cmPredicate = cmPredicate;
    }

    public void setTopicStore(TopicStore topicStore) {
        this.topicStore = topicStore;
    }


    void reconcile(ConfigMap cm, TopicName topicName) {
        Topic k8sTopic = TopicSerialization.fromConfigMap(cm);
        Future<Topic> topicResult = Future.future();
        Future<TopicMetadata> metadataResult = Future.future();
        kafka.topicMetadata(topicName, metadataResult.completer());
        topicStore.read(topicName, topicResult.completer());
        CompositeFuture.all(topicResult, metadataResult).setHandler(ar -> {
            Topic privateTopic = ar.result().resultAt(0);
            TopicMetadata kafkaTopicMeta = ar.result().resultAt(1);
            Topic kafkaTopic = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
            reconcile(cm, k8sTopic, kafkaTopic, privateTopic, reconcileResult -> {});
        });
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
    void reconcile(HasMetadata involvedObject,
                   Topic k8sTopic, Topic kafkaTopic, Topic privateTopic, Handler<AsyncResult<Void>> reconciliationResultHandler) {
        if (privateTopic == null) {
            class CreateInTopicStoreHandler implements Handler<AsyncResult<Void>>  {

                private final Topic source;

                CreateInTopicStoreHandler(Topic source) {
                    this.source = source;
                }

                @Override
                public void handle(AsyncResult<Void> ar) {
                    // In all cases, create in privateState
                    if (ar.succeeded()) {
                        enqueue(new CreateInTopicStore(topicStore, source, involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }
            }
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // All three null? This shouldn't be possible
                    logger.error("All three topics null during reconciliation. This should be impossible.");
                    return;
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    enqueue(new CreateConfigMap(kafkaTopic, new CreateInTopicStoreHandler(kafkaTopic)));

                }
            } else if (kafkaTopic == null) {
                // it's been created in k8s => create in Kafka and privateState
                enqueue(new CreateKafkaTopic(k8sTopic, new CreateInTopicStoreHandler(k8sTopic), involvedObject));
            } else if (TopicDiff.diff(kafkaTopic, k8sTopic).isEmpty()) {
                // they're the same => do nothing
                logger.debug("k8s and kafka versions of topic '{}' are the same", kafkaTopic.getTopicName());
                enqueue(new CreateInTopicStore(topicStore, kafkaTopic, involvedObject, reconciliationResultHandler));
            } else {
                // TODO use whichever has the most recent mtime
                throw new RuntimeException("Not implemented");
            }
        } else {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // delete privateState
                    enqueue(new DeleteFromTopicStore(topicStore, privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    enqueue(new DeleteKafkaTopic(kafkaTopic.getTopicName(), involvedObject, ar -> {
                        if (ar.succeeded()) {
                            enqueue(new DeleteFromTopicStore(topicStore, kafkaTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar);
                        }
                    }));

                }
            } else if (kafkaTopic == null) {
                // it was deleted in kafka so delete in k8s and privateState
                enqueue(new DeleteConfigMap(k8sTopic.getTopicName(), ar -> {
                    if (ar.succeeded()) {
                        enqueue(new DeleteFromTopicStore(topicStore, k8sTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            } else {
                // all three exist
                TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
                TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
                String conflict = oursKafka.conflict(oursK8s);
                if (conflict != null) {
                    enqueue(new ErrorEvent(involvedObject, "ConfigMap and Topic both changed in a conflicting way: " + conflict));
                    // TODO called reconciliationResultHandler, or push error handling into reconciliationResultHandler
                } else {
                    TopicDiff merged = oursKafka.merge(oursK8s);
                    Topic result = merged.apply(privateTopic);
                    if (merged.changesReplicationFactor()) {
                        enqueue(new ErrorEvent(involvedObject, "Topic Replication Factor cannot be changed"));
                        // TODO called reconciliationResultHandler, or push error handling into reconciliationResultHandler
                    } else {
                        enqueue(new UpdateConfigMap(result, ar -> {
                            // TODO chain these properly
                            if (merged.changesConfig()) {
                                enqueue(new UpdateKafkaConfig(result, involvedObject));
                            }
                            if (merged.changesNumPartitions()) {
                                enqueue(new UpdateKafkaPartitions(result, involvedObject));
                            }
                            enqueue(new UpdateInTopicStore(topicStore, result, involvedObject));
                            reconciliationResultHandler.handle(ar);
                        }, involvedObject));

                    }
                }
            }
        }
    }

    void enqueue(OperatorEvent event) {
        logger.info("Enqueuing event {}", event);
        vertx.runOnContext(event);
    }

    /** Called when a topic znode is deleted in ZK */
    void onTopicDeleted(TopicName topicName) {
        // XXX currently runs on the ZK thread, requiring a synchronized `inFlight`
        // is it better to put this check in the topic deleted event?
        // that would require exposing an API to remove()
        if (inFlight.shouldProcessDelete(topicName)) {
            enqueue(new DeleteConfigMap(topicName, ar -> {
                if (ar.succeeded()) {
                    enqueue(new DeleteFromTopicStore(topicStore, topicName, null, ar2 -> {}));
                }
            }));
        }
    }

    /** Called when a topic znode is created in ZK */
    void onTopicCreated(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        // XXX currently runs on the ZK thread, requiring a synchronized inFlight
        // is it better to put this check in the topic deleted event?
        if (inFlight.shouldProcessTopicCreate(topicName)) {
            Handler<AsyncResult<TopicMetadata>> handler = new Handler<AsyncResult<TopicMetadata>>() {
                BackOff backOff = new BackOff();

                @Override
                public void handle(AsyncResult<TopicMetadata> metadataResult) {
                    if (metadataResult.failed()) {
                        if (metadataResult.cause() instanceof UnknownTopicOrPartitionException) {
                            // In this case it is most likely that we've been notified by ZK
                            // before Kafka has finished creating the topic, so we retry
                            // with exponential backoff.
                            long delay;
                            try {
                                delay = backOff.delayMs();
                            } catch (MaxAttemptsExceededException e) {
                                resultHandler.handle(Future.failedFuture(e));
                                return;
                            }
                            if (delay < 1) {
                                // vertx won't tolerate a zero delay
                                vertx.runOnContext(timerId -> kafka.topicMetadata(topicName, this));
                            } else {
                                vertx.setTimer(TimeUnit.MILLISECONDS.convert(delay, TimeUnit.MILLISECONDS),
                                        timerId -> kafka.topicMetadata(topicName, this));
                            }
                        } else {
                            resultHandler.handle(Future.failedFuture(metadataResult.cause()));
                        }
                    } else {
                        // We now have the metadata we need to create the
                        // ConfigMap...
                        Topic topic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                        enqueue(new CreateConfigMap(topic, kubeResult -> {
                            if (kubeResult.succeeded()) {
                                enqueue(new CreateInTopicStore(topicStore, topic, null, resultHandler));
                            }
                        }));
                    }
                }
            };
            kafka.topicMetadata(topicName, handler);
        }
    }

    /** Called when a ConfigMap is added in k8s */
    void onConfigMapAdded(ConfigMap configMap, Handler<AsyncResult<Void>> resultHandler) {
        if (cmPredicate.test(configMap)) {
            TopicName topicName = new TopicName(configMap);
            if (inFlight.shouldProcessConfigMapAdded(topicName)) {
                Topic topic = TopicSerialization.fromConfigMap(configMap);
                enqueue(new CreateKafkaTopic(topic, ar -> {
                    if (ar.succeeded()) {
                        enqueue(new CreateInTopicStore(topicStore, topic, configMap, resultHandler));
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                }, configMap));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    /** Called when a ConfigMap is modified in k8s */
    void onConfigMapModified(TopicStore topicStore, ConfigMap configMap) {
        if (cmPredicate.test(configMap)) {
            TopicName topicName = new TopicName(configMap);
            if (inFlight.shouldProcessConfigMapModified(topicName)) {
                // We don't know what's changed in the ConfigMap
                // it could be #partitions and/or config and/or replication factor
                // So call reconcile, rather than enqueuing a UpdateKafkaTopic directly
                reconcile(configMap, topicName);
                //enqueue(new UpdateKafkaTopic(topic, configMap));
            }
        }
    }

    /** Called when a ConfigMap is deleted in k8s */
    void onConfigMapDeleted(ConfigMap configMap) {
        if (cmPredicate.test(configMap)) {
            TopicName topicName = new TopicName(configMap);
            if (inFlight.shouldProcessConfigMapDeleted(topicName)) {
                enqueue(new DeleteKafkaTopic(topicName, configMap, ar -> {
                    if (ar.succeeded()) {
                        enqueue(new DeleteFromTopicStore(topicStore, topicName, configMap, storeResult -> {}));
                    }
                }));
            }
        }
    }

    private class UpdateInTopicStore extends OperatorEvent {
        private final TopicStore topicStore;
        private final Topic topic;
        private final HasMetadata involvedObject;

        public UpdateInTopicStore(TopicStore topicStore, Topic topic, HasMetadata involvedObject) {
            this.topicStore = topicStore;
            this.topic = topic;
            this.involvedObject = involvedObject;
        }

        @Override
        public void process() throws OperatorException {
            topicStore.update(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString()));
                }
            });
        }

        @Override
        public String toString() {
            return "UpdateInTopicStore(topicName="+topic.getTopicName()+")";
        }
    }

    class CreateInTopicStore extends OperatorEvent {
        private final TopicStore topicStore;
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        private CreateInTopicStore(TopicStore topicStore, Topic topic, HasMetadata involvedObject,
                                   Handler<AsyncResult<Void>> handler) {
            this.topicStore = topicStore;
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void process() throws OperatorException {
            topicStore.create(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString()));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "CreateInTopicStore(topicName="+topic.getTopicName()+")";
        }
    }

    class DeleteFromTopicStore extends OperatorEvent {
        private final TopicStore topicStore;
        private final TopicName topicName;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        private DeleteFromTopicStore(TopicStore topicStore, TopicName topicName, HasMetadata involvedObject, 
                                     Handler<AsyncResult<Void>> handler) {
            this.topicStore = topicStore;
            this.topicName = topicName;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void process() throws OperatorException {
            topicStore.delete(topicName, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString()));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "DeleteFromTopicStore(topicName="+topicName+")";
        }
    }
}

