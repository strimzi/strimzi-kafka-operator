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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;


public class Operator {

    private final static Logger logger = LoggerFactory.getLogger(Operator.class);

    private KubernetesClient client;
    private AdminClient adminClient;

    enum State {
        CM_CREATE,
        CM_DELETE,
        CM_UPDATE,
        TOPIC_CREATE,
        TOPIC_DELETE,
        TOPIC_UPDATE
    }
    // track changes we caused, so we don't try to update zk for a cm change we
    // make because of a zk change... They're accessed by the both the
    // ZK and topic-controller-executor threads
    private final Map<TopicName, State> pending = Collections.synchronizedMap(new HashMap());

    /** Executor for processing {@link OperatorEvent}s. */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("topic-operator-executor");
            return t;
        }
    });

    public Operator(String kubernetesMasterUrl, String kafkaBootstrapServers, String zookeeperConnect) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrapServers);
        adminClient = AdminClient.create(props);

        final io.fabric8.kubernetes.client.Config config = new ConfigBuilder().withMasterUrl(kubernetesMasterUrl).build();
        client = new DefaultKubernetesClient(config);

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
    }


    void createConfigMap(ConfigMap cm) {
        // TODO assert no existing mapping
        pending.put(new TopicName(cm), State.CM_CREATE);
        client.configMaps().create(cm);
    }

    void updateConfigMap(ConfigMap cm) {
        // TODO assert no existing mapping
        pending.put(new TopicName(cm), State.CM_UPDATE);
        client.configMaps().createOrReplace(cm);
    }

    void deleteConfigMap(TopicName topicName) {
        // TODO assert no existing mapping
        pending.put(topicName, State.CM_DELETE);
        // Delete the CM by the topic name, because neither ZK nor Kafka know the CM name
        client.configMaps().withField("name", topicName.toString()).delete();
    }

    private void enqueue(OperatorEvent event) {
        logger.info("Enqueuing event {}", event);
        executor.execute(event);
    }

    /** Called when a topic znode is deleted in ZK */
    void onTopicDeleted(TopicName topicName) {
        // XXX currently runs on the ZK thread, requiring a synchronized `pending`
        // is it better to put this check in the topic deleted event?
        // that would require exposing an API to remove()
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.TOPIC_DELETE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("Topic {} was deleted by me, so no need to reconcile", topicName);
            }
        } else {
            enqueue(new OperatorEvent.TopicDeleted(this, topicName));
        }
    }

    /** Called when a topic znode is created in ZK */
    void onTopicCreated(TopicName topicName) {
        // XXX currently runs on the ZK thread, requiring a synchronized pending
        // is it better to put this check in the topic deleted event?
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.TOPIC_CREATE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("Topic {} was created by me, so no need to reconcile", topicName);
            }
        } else {
            ResultHandler<TopicMetadata> handler = new ResultHandler<TopicMetadata>() {

                final BackOff backoff = new BackOff();

                @Override
                public void handleResult(AsyncResult<? extends TopicMetadata> result) throws OperatorException {
                    if (result.isSuccess()) {
                        TopicMetadata metadata = result.result();
                        enqueue(new OperatorEvent.TopicCreated(Operator.this, topicName, metadata.getDescription(), metadata.getConfig()));
                    } else {

                        if (result.exception() instanceof UnknownTopicOrPartitionException) {
                            // retry: It's possible the KafkaController, although it's created the path in ZK,
                            // hasn't finished creating the topic yet.
                            topicMetadata(topicName, this, backoff.delayMs(), TimeUnit.MILLISECONDS);
                        } else {
                            logger.debug("Got error for describing topic and/or getting topic config for topic {}", topicName, result.exception());
                            // TODO what? We need to create an event in k8s
                        }
                    }
                }
            };
            topicMetadata(topicName, handler, 0, TimeUnit.MILLISECONDS);
        }
    }

    /** Called when a ConfigMap is added in k8s */
    private void onConfigMapAdded(ConfigMap configMap) {
        MapName mapName = new MapName(configMap);
        TopicName topicName = new TopicName(configMap);
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.CM_CREATE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("ConfigMap {} was created by me, so no need to reconcile", mapName);
            }
        } else {
            enqueue(new OperatorEvent.ConfigMapCreated(this, configMap));
        }
    }

    /** Called when a ConfigMap is modified in k8s */
    private void onConfigMapModified(ConfigMap configMap) {
        MapName mapName = new MapName(configMap);
        TopicName topicName = new TopicName(configMap);
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.CM_UPDATE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("ConfigMap {} was modified by me, so no need to reconcile", mapName);
            }
        } else {
            enqueue(new OperatorEvent.ConfigMapModified(this, configMap));
        }
    }

    /** Called when a ConfigMap is deleted in k8s */
    private void onConfigMapDeleted(ConfigMap configMap) {
        MapName mapName = new MapName(configMap);
        TopicName topicName = new TopicName(configMap);
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.CM_DELETE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("ConfigMap {} was deleted by me, so no need to reconcile", mapName);
            }
        } else {
            enqueue(new OperatorEvent.ConfigMapDeleted(this, configMap));
        }
    }

    /**
     * Create the given k8s event
     */
    void createEvent(Event event) {
        try {
            //logger.debug("Creating event {}", event);
            //client.events().create(outcomeEvent);
        } catch (KubernetesClientException e) {
            logger.error("Error creating event {}", event, e);
        }
    }


    abstract class Work implements Runnable {
        @Override
        public void run() {
            if (!complete()) {
                executor.execute(this);
            }
        }

        protected abstract boolean complete();
    }

    /** Some work that depends on a single future */
    class UniWork<T> extends Work {
        private final KafkaFuture<T> future;
        private final ResultHandler<T> handler;

        public UniWork(KafkaFuture<T> future, ResultHandler<T> handler) {
            if (future == null) {
                throw new NullPointerException();
            }
            if (handler == null) {
                throw new NullPointerException();
            }
            this.future = future;
            this.handler = handler;
        }

        @Override
        protected boolean complete() {
            if (this.future.isDone()) {
                logger.debug("Future {} of work {} is done", future, this);
                try {
                    try {
                        T result = this.future.get();
                        logger.debug("Future {} has result {}", future, result);
                        this.handler.handleResult(AsyncResult.success(result));
                        logger.debug("Handler for work {} executed ok", this);
                    } catch (ExecutionException e) {
                        logger.debug("Future {} threw {}", future, e.toString());
                        this.handler.handleResult(AsyncResult.failure(e.getCause()));
                    } catch (InterruptedException e) {
                        logger.debug("Future {} threw {}", future, e.toString());
                        this.handler.handleResult(AsyncResult.failure(e));
                    }
                } catch (OperatorException e) {
                    // TODO handler threw, but I have no context for creating a k8s error event
                    logger.debug("Handler for work {} threw {}", this, e.toString());
                    e.printStackTrace();
                }
                return true;
            } else {
                logger.debug("Future {} is not done", future);
                return false;
            }
        }
    }

    /** Some work that depends on two futures */
    class BiWork<T, U, R> extends Work {
        private final KafkaFuture<T> futureT;
        private final KafkaFuture<U> futureU;
        private final BiFunction<T, U, R> combiner;
        private final ResultHandler<R> handler;

        public BiWork(KafkaFuture<T> futureT, KafkaFuture<U> futureU, BiFunction<T, U, R> combiner, ResultHandler<R> handler) {
            if (futureT == null) {
                throw new NullPointerException();
            }
            if (futureU == null) {
                throw new NullPointerException();
            }
            if (combiner == null) {
                throw new NullPointerException();
            }
            if (handler == null) {
                throw new NullPointerException();
            }
            this.futureT = futureT;
            this.futureU = futureU;
            this.combiner = combiner;
            this.handler = handler;
        }

        @Override
        protected boolean complete() {
            if (this.futureT.isDone()
                    && this.futureU.isDone()) {
                try {
                    final T resultT;
                    try {
                        resultT = this.futureT.get();
                        logger.debug("Future {} has result {}", futureT, resultT);
                    } catch (ExecutionException e) {
                        logger.debug("Future {} threw {}", futureT, e.toString());
                        this.handler.handleResult(AsyncResult.failure(e.getCause()));
                        return true;
                    } catch (InterruptedException e) {
                        logger.debug("Future {} threw {}", futureT, e.toString());
                        this.handler.handleResult(AsyncResult.failure(e));
                        return true;
                    }
                    final U resultU;
                    try {
                        resultU = this.futureU.get();
                        logger.debug("Future {} has result {}", futureU, resultU);
                    } catch (ExecutionException e) {
                        logger.debug("Future {} threw {}", futureT, e.toString());
                        this.handler.handleResult(AsyncResult.failure(e.getCause()));
                        return true;
                    } catch (InterruptedException e) {
                        logger.debug("Future {} threw {}", futureT, e.toString());
                        this.handler.handleResult(AsyncResult.failure(e));
                        return true;
                    }

                    this.handler.handleResult(AsyncResult.success(combiner.apply(resultT, resultU)));
                    logger.debug("Handler for work {} executed ok", this);
                } catch (OperatorException e) {
                    // TODO handler threw, but I have no context for creating a k8s error event
                    logger.debug("Handler for work {} threw {}", this, e.toString());
                    e.printStackTrace();
                }

                return true;
            } else {
                if (!this.futureT.isDone())
                    logger.debug("Future {} is not done", futureT);
                if (!this.futureU.isDone())
                    logger.debug("Future {} is not done", futureU);
                return false;
            }
        }
    }

    /**
     * Queue a future and callback. The callback will be invoked (on a separate thread)
     * when the future is ready.
     */
    private void queueWork(Work work) {
        queueWork(work, 0, TimeUnit.MILLISECONDS);
    }
    private void queueWork(Work work, long delay, TimeUnit unit) {
        if (delay != 0) {
            logger.debug("Queuing work {} for execution in {} {}", work, delay, unit);
            executor.schedule(work, delay, unit);
        } else {
            logger.debug("Queuing work {} for immediate execution", work);
            executor.execute(work);
        }
    }

    /**
     * Create a new topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    void createTopic(NewTopic newTopic, ResultHandler<Void> handler) {
        // TODO assert no existing mapping
        pending.put(new TopicName(newTopic.name()), State.TOPIC_CREATE);
        logger.debug("Creating topic {}", newTopic);
        KafkaFuture<Void> future = adminClient.createTopics(
                Collections.singleton(newTopic)).values().get(newTopic.name());
        queueWork(new UniWork<>(future, handler));
    }

    /**
     * Delete a topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    void deleteTopic(TopicName topicName, ResultHandler<Void> handler) {
        // TODO assert no existing mapping
        pending.put(topicName, State.TOPIC_DELETE);
        logger.debug("Deleting topic {}", topicName);
        KafkaFuture<Void> future = adminClient.deleteTopics(
                Collections.singleton(topicName.toString())).values().get(topicName);
        queueWork(new UniWork<>(future, handler));
    }

    /**
     * Describe a topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    void describeTopic(TopicName topicName, ResultHandler<TopicDescription> handler) {
        logger.debug("Describing topic {}", topicName);
        KafkaFuture<TopicDescription> future = adminClient.describeTopics(
                Collections.singleton(topicName.toString())).values().get(topicName);
        queueWork(new UniWork<>(future, handler));
    }

    /**
     * Get a topic config via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    void topicConfig(TopicName topicName, ResultHandler<org.apache.kafka.clients.admin.Config> handler) {
        logger.debug("Getting config for topic {}", topicName);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.toString());
        KafkaFuture<org.apache.kafka.clients.admin.Config> future = adminClient.describeConfigs(
                Collections.singleton(resource)).values().get(resource);
        queueWork(new UniWork<>(future, handler));
    }

    /**
     * Get a topic config via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    void topicMetadata(TopicName topicName, ResultHandler<TopicMetadata> handler, long delay, TimeUnit unit) {
        logger.debug("Getting metadata for topic {}", topicName);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.toString());
        KafkaFuture<Config> configFuture = adminClient.describeConfigs(
                Collections.singleton(resource)).values().get(resource);
        KafkaFuture<TopicDescription> descriptionFuture = adminClient.describeTopics(
                Collections.singleton(topicName.toString())).values().get(topicName);
        queueWork(new BiWork<>(descriptionFuture, configFuture,
                (desc, conf) -> new TopicMetadata(desc, conf),
                handler), delay, unit);
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
}

