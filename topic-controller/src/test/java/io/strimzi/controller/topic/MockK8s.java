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
import java.util.function.Function;
import java.util.function.Predicate;

public class MockK8s implements K8s {

    private Map<MapName, ConfigMap> byName = new HashMap<>();
    private Map<TopicName, ConfigMap> byTopicName = new HashMap<>();
    private List<Event> events = new ArrayList<>();
    private Function<MapName, AsyncResult<Void>> createResponse = n -> Future.failedFuture("Unexpected. ");
    private Function<MapName, AsyncResult<Void>> modifyResponse = n -> Future.failedFuture("Unexpected. ");
    private Function<TopicName, AsyncResult<Void>> deleteResponse = n -> Future.failedFuture("Unexpected. ");

    public MockK8s setCreateResponse(MapName mapName, Exception exception) {
        Function<MapName, AsyncResult<Void>> old = createResponse;
        createResponse = n -> {
            if (mapName.equals(n)) {
                if (exception == null) {
                    return Future.succeededFuture();
                } else {
                    return Future.failedFuture(exception);
                }
            }
            return old.apply(n);
        };
        return this;
    }

    public MockK8s setModifyResponse(MapName mapName, Exception exception) {
        Function<MapName, AsyncResult<Void>> old = modifyResponse;
        modifyResponse = n -> {
            if (mapName.equals(n)) {
                if (exception == null) {
                    return Future.succeededFuture();
                } else {
                    return Future.failedFuture(exception);
                }
            }
            return old.apply(n);
        };
        return this;
    }

    public MockK8s setDeleteResponse(TopicName mapName, Exception exception) {
        Function<TopicName, AsyncResult<Void>> old = deleteResponse;
        deleteResponse = n -> {
            if (mapName.equals(n)) {
                if (exception == null) {
                    return Future.succeededFuture();
                } else {
                    return Future.failedFuture(exception);
                }
            }
            return old.apply(n);
        };
        return this;
    }

    @Override
    public void createConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = createResponse.apply(new MapName(cm));
        if (response.succeeded()) {
            ConfigMap old = byName.put(new MapName(cm), cm);
            if (old == null) {
                byTopicName.put(new TopicName(cm.getData().getOrDefault(TopicSerialization.CM_KEY_NAME, cm.getMetadata().getName())), cm);
            } else {
                handler.handle(Future.failedFuture("configmap already existed: " + cm.getMetadata().getName()));
                return;
            }
        }
        handler.handle(response);
    }

    @Override
    public void updateConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = modifyResponse.apply(new MapName(cm));
        if (response.succeeded()) {
            ConfigMap old = byName.put(new MapName(cm), cm);
            if (old == null) {
                handler.handle(Future.failedFuture("configmap does not exist, cannot be updated: " + cm.getMetadata().getName()));
                return;
            } else {
                byTopicName.put(new TopicName(cm.getData().getOrDefault(TopicSerialization.CM_KEY_NAME, cm.getMetadata().getName())), cm);
            }
        }
        handler.handle(response);
    }

    @Override
    public void deleteConfigMap(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = deleteResponse.apply(topicName);
        if (response.succeeded()) {
            ConfigMap cm = byTopicName.remove(topicName);
            if (cm == null) {
                handler.handle(Future.failedFuture("No such configmap, with topic name " + topicName));
                return;
            } else {
                byName.remove(new MapName(cm.getMetadata().getName()));
            }
        }
        handler.handle(response);
    }

    @Override
    public void listMaps(Handler<AsyncResult<List<ConfigMap>>> handler) {
        handler.handle(Future.succeededFuture(new ArrayList(byName.values())));
    }

    @Override
    public void getFromName(MapName mapName, Handler<AsyncResult<ConfigMap>> handler) {
        ConfigMap cm = byName.get(mapName);
        handler.handle(Future.succeededFuture(cm));
    }

    @Override
    public void createEvent(Event event, Handler<AsyncResult<Void>> handler) {
        events.add(event);
        handler.handle(Future.succeededFuture());
    }

    public void assertExists(TestContext context, MapName mapName) {
        context.assertTrue(byName.containsKey(mapName));
    }

    public void assertContains(TestContext context, ConfigMap cm) {
        ConfigMap configMap = byName.get(new MapName(cm));
        context.assertEquals(cm, configMap);
    }

    public void assertNotExists(TestContext context, MapName mapName) {
        context.assertFalse(byName.containsKey(mapName));
    }

    public void assertContainsEvent(TestContext context, Predicate<Event> test) {
        for (Event event : events) {
            if (test.test(event)) {
                return;
            }
        }
        context.fail("Missing event");
    }

    public void assertNoEvents(TestContext context) {
        context.assertTrue(events.isEmpty());
    }
}
