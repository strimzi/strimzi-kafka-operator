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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Partial implementation of {@link Kafka} omitting those methods which imply a partition assignment.
 * Subclasses will need to implement those method according to their own semantics.
 * For example it is anticipated that one subclass will delegate to a "cluster balancer" so that cluster-wide,
 * traffic-aware assignments can be done.
 */
public abstract class BaseKafkaImpl implements Kafka {

    private final static Logger logger = LoggerFactory.getLogger(BaseKafkaImpl.class);

    protected final AdminClient adminClient;

    protected final Vertx vertx;

    public BaseKafkaImpl(AdminClient adminClient, Vertx vertx) {
        this.adminClient = adminClient;
        this.vertx = vertx;
    }

    abstract class Work implements Runnable, Handler<Void> {
        @Override
        public void run() {
            if (!complete()) {
                vertx.runOnContext(this);
            }
        }

        @Override
        public void handle(Void v) {
            run();
        }

        protected abstract boolean complete();
    }

    /** Some work that depends on a single future */
    class UniWork<T> extends Work {
        private final KafkaFuture<T> future;
        private final Handler<AsyncResult<T>> handler;

        public UniWork(KafkaFuture<T> future, Handler<AsyncResult<T>> handler) {
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
                        this.handler.handle(Future.succeededFuture(result));
                        logger.debug("Handler for work {} executed ok", this);
                    } catch (ExecutionException e) {
                        logger.debug("Future {} threw {}", future, e.toString());
                        this.handler.handle(Future.failedFuture(e.getCause()));
                    } catch (InterruptedException e) {
                        logger.debug("Future {} threw {}", future, e.toString());
                        this.handler.handle(Future.failedFuture(e));
                    }
                } catch (ControllerException e) {
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
    class MetadataWork extends Work {
        private final KafkaFuture<TopicDescription> descFuture;
        private final KafkaFuture<Config> configFuture;
        private final Handler<AsyncResult<TopicMetadata>> handler;
        private boolean handled = false;

        public MetadataWork(KafkaFuture<TopicDescription> descFuture,
                            KafkaFuture<Config> configFuture, Handler<AsyncResult<TopicMetadata>> handler) {
            if (descFuture == null) {
                throw new NullPointerException();
            }
            if (configFuture == null) {
                throw new NullPointerException();
            }
            if (handler == null) {
                throw new NullPointerException();
            }
            this.descFuture = descFuture;
            this.configFuture = configFuture;
            this.handler = handler;
        }


        private <T> T result(KafkaFuture<T> future) {
            T result;
            try {
                result = future.get();
                logger.debug("Future {} has result {}", future, result);
            } catch (ExecutionException e) {
                logger.debug("Future {} threw {}", future, e.toString());
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    result = null;
                } else{
                    handler.handle(Future.failedFuture(e.getCause()));
                    handled = true;
                    return null;
                }
            } catch (InterruptedException e) {
                logger.debug("Future {} threw {}", future, e.toString());
                handler.handle(Future.failedFuture(e));
                handled = true;
                return null;
            }
            return result;
        }

        @Override
        protected boolean complete() {
            if (this.descFuture.isDone()
                    && this.configFuture.isDone()) {
                final TopicDescription desc = result(this.descFuture);
                final Config config = result(this.configFuture);
                if (!handled) {
                    TopicMetadata metadata;
                    if (desc != null && config != null) {
                        metadata = new TopicMetadata(desc, config);
                    } else {
                        metadata = null;
                    }
                    this.handler.handle(Future.succeededFuture(metadata));
                    this.handled = true;
                    logger.debug("Handler for work {} executed ok", this);
                    return true;
                } else {
                    return false;
                }
            } else {
                if (!this.descFuture.isDone())
                    logger.debug("Description future {} is not done", descFuture);
                if (!this.configFuture.isDone())
                    logger.debug("Config future {} is not done", configFuture);
                return false;
            }
        }
    }

    /**
     * Queue a future and callback. The callback will be invoked (on a separate thread)
     * when the future is ready.
     */
    protected void queueWork(Work work) {
        logger.debug("Queuing work {} for immediate execution", work);
        vertx.runOnContext(work);
    }

    /**
     * Delete a topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    @Override
    public void deleteTopic(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        logger.debug("Deleting topic {}", topicName);
        KafkaFuture<Void> future = adminClient.deleteTopics(
                Collections.singleton(topicName.toString())).values().get(topicName);
        queueWork(new UniWork<>(future, handler));
    }

    @Override
    public void updateTopicConfig(Topic topic, Handler<AsyncResult<Void>> handler) {
        Map<ConfigResource, Config> configs = TopicSerialization.toTopicConfig(topic);
        KafkaFuture<Void> future = adminClient.alterConfigs(configs).values().get(configs.keySet().iterator().next());
        queueWork(new UniWork<>(future, handler));
    }

    /**
     * Get a topic config via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    @Override
    public void topicMetadata(TopicName topicName, Handler<AsyncResult<TopicMetadata>> handler) {
        logger.debug("Getting metadata for topic {}", topicName);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.toString());
        KafkaFuture<TopicDescription> descriptionFuture = adminClient.describeTopics(
                Collections.singleton(topicName.toString())).values().get(topicName.toString());
        KafkaFuture<Config> configFuture = adminClient.describeConfigs(
                Collections.singleton(resource)).values().get(resource);
        queueWork(new MetadataWork(descriptionFuture,
                    configFuture,
                    result -> handler.handle(result)));
    }

    @Override
    public void listTopics(Handler<AsyncResult<Set<String>>> handler) {
        ListTopicsResult future = adminClient.listTopics();
        queueWork(new UniWork<>(future.names(), handler));
    }


}
