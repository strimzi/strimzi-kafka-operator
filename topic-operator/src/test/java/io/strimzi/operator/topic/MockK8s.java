/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.api.kafka.model.KafkaTopic;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MockK8s implements K8s {

    private Map<MapName, AsyncResult<KafkaTopic>> byName = new HashMap<>();
    private List<Event> events = new ArrayList<>();
    private Function<MapName, AsyncResult<Void>> createResponse = n -> Future.failedFuture("Unexpected. ");
    private Function<MapName, AsyncResult<Void>> modifyResponse = n -> Future.failedFuture("Unexpected. ");
    private Function<MapName, AsyncResult<Void>> deleteResponse = n -> Future.failedFuture("Unexpected. ");
    private Supplier<AsyncResult<List<KafkaTopic>>> listResponse = () -> Future.succeededFuture(new ArrayList(byName.values().stream().filter(ar -> ar.succeeded()).map(ar -> ar.result()).collect(Collectors.toList())));

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

    public MockK8s setDeleteResponse(MapName mapName, Exception exception) {
        Function<MapName, AsyncResult<Void>> old = deleteResponse;
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
    public void createConfigMap(KafkaTopic cm, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = createResponse.apply(new MapName(cm));
        if (response.succeeded()) {
            AsyncResult<KafkaTopic> old = byName.put(new MapName(cm), Future.succeededFuture(cm));
            if (old != null) {
                handler.handle(Future.failedFuture("configmap already existed: " + cm.getMetadata().getName()));
                return;
            }
        }
        handler.handle(response);
    }

    @Override
    public void updateConfigMap(KafkaTopic cm, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = modifyResponse.apply(new MapName(cm));
        if (response.succeeded()) {
            AsyncResult<KafkaTopic> old = byName.put(new MapName(cm), Future.succeededFuture(cm));
            if (old == null) {
                handler.handle(Future.failedFuture("configmap does not exist, cannot be updated: " + cm.getMetadata().getName()));
                return;
            }
        }
        handler.handle(response);
    }

    @Override
    public void deleteConfigMap(MapName mapName, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = deleteResponse.apply(mapName);
        if (response.succeeded()) {
            if (byName.remove(mapName) == null) {
                handler.handle(Future.failedFuture("configmap does not exist, cannot be deleted: " + mapName));
                return;
            }
        }
        handler.handle(response);
    }

    @Override
    public void listMaps(Handler<AsyncResult<List<KafkaTopic>>> handler) {
        handler.handle(listResponse.get());
    }

    public void setListMapsResult(Supplier<AsyncResult<List<KafkaTopic>>> response) {
        this.listResponse = response;
    }

    @Override
    public void getFromName(MapName mapName, Handler<AsyncResult<KafkaTopic>> handler) {
        AsyncResult<KafkaTopic> cmFuture = byName.get(mapName);
        handler.handle(cmFuture != null ? cmFuture : Future.succeededFuture());
    }

    @Override
    public void createEvent(Event event, Handler<AsyncResult<Void>> handler) {
        events.add(event);
        handler.handle(Future.succeededFuture());
    }

    public void assertExists(TestContext context, MapName mapName) {
        AsyncResult<KafkaTopic> got = byName.get(mapName);
        context.assertTrue(got != null && got.succeeded());
    }

    public void assertContains(TestContext context, KafkaTopic cm) {
        AsyncResult<KafkaTopic> configMapResult = byName.get(new MapName(cm));
        context.assertTrue(configMapResult.succeeded());
        context.assertEquals(cm, configMapResult.result());
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

    public void setGetFromNameResponse(MapName mapName, AsyncResult<KafkaTopic> futureCm) {
        this.byName.put(mapName, futureCm);
    }
}
