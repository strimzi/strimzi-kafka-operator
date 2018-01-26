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
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link Controller#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher extends ZkWatcher {

    private static final String CONFIGS_ZNODE = "/config/topics";

    TopicConfigsWatcher(Controller controller) {
        super(controller, CONFIGS_ZNODE);
    }

    @Override
    protected void notifyController(String child) {
        log.debug("Config change for topic {}", child);
        controller.onTopicConfigChanged(new TopicName(child), ar2 -> {
            log.info("Reconciliation result due to topic config change: {}", ar2);
        });
    }
}
