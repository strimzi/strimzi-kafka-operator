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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.unit.TestContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockK8s implements K8s {

    private Map<MapName, ConfigMap> byName = new HashMap<>();
    private Map<TopicName, ConfigMap> byTopicName = new HashMap<>();

    @Override
    public void createConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        ConfigMap old = byName.put(new MapName(cm.getMetadata().getName()), cm);
        if (old == null) {
            byTopicName.put(new TopicName(cm.getData().getOrDefault(TopicSerialization.CM_KEY_NAME, cm.getMetadata().getName())), cm);
            handler.handle(Future.succeededFuture());
        } else {
            handler.handle(Future.failedFuture("configmap already existed: " + cm.getMetadata().getName()));
        }
    }

    @Override
    public void updateConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        ConfigMap old = byName.put(new MapName(cm.getMetadata().getName()), cm);
        if (old == null) {
            handler.handle(Future.failedFuture("configmap does not exists, cannot be updated: " + cm.getMetadata().getName()));
        } else {
            byTopicName.put(new TopicName(cm.getData().getOrDefault(TopicSerialization.CM_KEY_NAME, cm.getMetadata().getName())), cm);
            handler.handle(Future.succeededFuture());
        }
    }

    @Override
    public void deleteConfigMap(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        ConfigMap cm = byTopicName.remove(topicName);
        if (cm == null) {
            handler.handle(Future.failedFuture("No such configmap, with topic name " + topicName));
        } else {
            byName.remove(new MapName(cm.getMetadata().getName()));
            handler.handle(Future.succeededFuture());
        }
    }

    @Override
    public void listMaps(Handler<AsyncResult<List<ConfigMap>>> handler) {
        handler.handle(Future.succeededFuture(new ArrayList(byName.values())));
    }

    @Override
    public void getFromName(MapName mapName, Handler<AsyncResult<ConfigMap>> handler) {
        ConfigMap cm = byName.get(mapName.toString());
        if (cm != null) {
            handler.handle(Future.succeededFuture(cm));
        } else {
            handler.handle(Future.failedFuture("No such configmap " + mapName));
        }
    }

    @Override
    public void createEvent(Event event, Handler<AsyncResult<Void>> handler) {
        throw new RuntimeException("Implement this mock!");
    }

    public void assertExists(TestContext context, MapName topicName) {
        context.assertTrue(byName.containsKey(topicName));
    }

    public void assertContains(TestContext context, ConfigMap cm) {
        ConfigMap configMap = byName.get(new MapName(cm));
        context.assertEquals(cm, configMap);
    }

    public void assertNotExists(TestContext context, MapName topicName) {
        context.assertFalse(byName.containsKey(topicName));
    }
}
