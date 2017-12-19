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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link Kafka} which leave partition assignment decisions to the Kafka controller.
 * The controller is able to make rack-aware assignments (if so configured), but does not take into account
 * other aspects (e.g. disk utilisation, CPU load, network IO).
 */
public class ControllerAssignedKafkaImpl extends BaseKafkaImpl {

    private final static Logger logger = LoggerFactory.getLogger(ControllerAssignedKafkaImpl.class);

    public ControllerAssignedKafkaImpl(AdminClient adminClient, Vertx vertx) {
        super(adminClient, vertx);
    }

    @Override
    public void increasePartitions(Topic topic, Handler<AsyncResult<Void>> handler) {
        final NewPartitions newPartitions = NewPartitions.increaseTo(topic.getNumPartitions());
        final Map<String, NewPartitions> request = Collections.singletonMap(topic.getTopicName().toString(), newPartitions);
        KafkaFuture<Void> future = adminClient.createPartitions(request).values().get(topic.getTopicName());
        queueWork(new UniWork<>(future, handler));
    }

    /**
     * Create a new topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    @Override
    public void createTopic(Topic topic, Handler<AsyncResult<Void>> handler) {
        NewTopic newTopic = TopicSerialization.toNewTopic(topic, null);

        logger.debug("Creating topic {}", newTopic);
        KafkaFuture<Void> future = adminClient.createTopics(
                Collections.singleton(newTopic)).values().get(newTopic.name());
        queueWork(new UniWork<>(future, handler));
    }

    @Override
    public void changeReplicationFactor(Topic topic, Handler<AsyncResult<Void>> handler) {

    }
}

