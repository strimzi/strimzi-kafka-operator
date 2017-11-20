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

import java.util.concurrent.CompletableFuture;

/**
 * Represents a persistent data store where the operator can store its copy of the
 * topic state that won't be modified by either K8S or Kafka.
 */
public interface TopicStore {
    /**
     * Get the topic with the given name.
     * @param name The name of the topic to get.
     * @return A future for the stored topic, which will return null if the topic didn't exist.
     */
    CompletableFuture<Topic> read(TopicName name);

    /**
     * Persist the given topic in the store.
     * @param topic The topic to persist.
     * @return A future for the success or failure of the operation.
     */
    CompletableFuture<Void> create(Topic topic);

    /**
     * Update the given topic in the store.
     * @param topic The topic to update.
     * @return A future for the success or failure of the operation.
     */
    CompletableFuture<Void> update(Topic topic);

    /**
     * Delete the given topic from the store.
     * @param topic The topic to delete.
     * @return A future for the success or failure of the operation.
     */
    CompletableFuture<Void> delete(TopicName topic);
}

