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

import org.apache.kafka.clients.admin.NewTopic;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface Kafka {
    void createTopic(NewTopic newTopic, ResultHandler<Void> handler);

    void deleteTopic(TopicName topicName, ResultHandler<Void> handler);

    void updateTopicConfig(Topic topic, ResultHandler<Void> handler);

    void increasePartitions(Topic topic, ResultHandler<Void> handler);

    CompletableFuture<TopicMetadata> topicMetadata(TopicName topicName, long delay, TimeUnit unit);

    void listTopics(ResultHandler<Set<String>> handler);
    CompletableFuture<Set<TopicName>> listTopicsFuture();

}

