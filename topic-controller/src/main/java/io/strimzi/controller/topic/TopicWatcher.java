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

import io.vertx.core.Handler;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 * calling {@link Controller#onTopicPartitionsChanged(TopicName, Handler)}
 * for changed children.
 */
public class TopicWatcher extends ZkWatcher {

    private static final String TOPICS_ZNODE = "/brokers/topics";

    TopicWatcher(Controller controller) {
        super(controller, TOPICS_ZNODE);
    }

    @Override
    protected void notifyController(String child) {
        log.debug("Partitions change for topic {}", child);
        controller.onTopicPartitionsChanged(new TopicName(child), ar -> {
            log.info("Reconciliation result due to topic partitions change: {}", ar);
        });
    }
}
