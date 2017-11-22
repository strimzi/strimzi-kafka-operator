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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Keeps track of all the in-flight changes so we don't respond to changes we ourselves initiated.
 */
public class InFlight {

    // TODO Could this actually check that the states of the topics match?
    // Because right now there can be a race if both ends get changed at about
    // the same time. In practice this is unlikely to be a problem, but it
    // would be good to eliminiate it

    private final static Logger logger = LoggerFactory.getLogger(InFlight.class);

    public void startCreatingTopic(TopicName topicName) {
        pending.put(topicName, State.TOPIC_CREATE);
    }

    public void startDeletingTopic(TopicName topicName) {
        pending.put(topicName, State.TOPIC_DELETE);
    }

    public boolean shouldProcessDelete(TopicName topicName) {
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.TOPIC_DELETE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("Topic {} was deleted by me, so no need to reconcile", topicName);
            }
            return false;
        } else {
            return true;
        }
    }

    public boolean shouldProcessTopicCreate(TopicName topicName) {
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.TOPIC_CREATE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("Topic {} was created by me, so no need to reconcile", topicName);
            }
            return false;
        } else {
            return true;
        }
    }

    public boolean shouldProcessConfigMapAdded(TopicName topicName) {
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.CM_CREATE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("ConfigMap for topic {} was created by me, so no need to reconcile", topicName);
            }
            return false;
        } else {
            return true;
        }
    }

    public boolean shouldProcessConfigMapModified(TopicName topicName) {
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.CM_UPDATE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("ConfigMap for topic {} was modified by me, so no need to reconcile", topicName);
            }
            return false;
        } else {
            return true;
        }
    }

    public boolean shouldProcessConfigMapDeleted(TopicName topicName) {
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.CM_DELETE) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("ConfigMap for topic {} was deleted by me, so no need to reconcile", topicName);
            }
            return false;
        } else {
            return true;
        }
    }

    public boolean shouldProcessTopicConfigChange(TopicName topicName) {
        State pendingState = pending.remove(topicName);
        if (pendingState != null) {
            if (pendingState != State.TOPIC_UPDATE_CONFIG) {
                logger.error("This shouldn't happen", topicName);
            } else {
                logger.info("Config change for topic {} was initiated by me, so no need to reconcile", topicName);
            }
            return false;
        } else {
            return true;
        }
    }

    enum State {
        CM_CREATE,
        CM_DELETE,
        CM_UPDATE,
        TOPIC_CREATE,
        TOPIC_DELETE,
        TOPIC_UPDATE_CONFIG
    }
    // track changes we caused, so we don't try to update zk for a cm change we
    // make because of a zk change... They're accessed by the both the
    // ZK and topic-controller-executor threads
    private final Map<TopicName, State> pending = Collections.synchronizedMap(new HashMap());

    public void startUpdatingConfigMap(ConfigMap cm) {
        // Assert no existing mapping
        pending.put(new TopicName(cm), State.CM_UPDATE);
    }

    public void startCreatingConfigMap(ConfigMap cm) {
        // Assert no existing mapping
        pending.put(new TopicName(cm), State.CM_CREATE);
    }

    public void startDeletingConfigMap(TopicName topicName) {
        // Assert no existing mapping
        pending.put(topicName, State.CM_DELETE);
    }
}
