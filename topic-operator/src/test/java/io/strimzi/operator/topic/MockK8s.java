/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.api.kafka.model.status.KafkaTopicStatusBuilder;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxTestContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MockK8s implements K8s {

    private Map<ResourceName, AsyncResult<KafkaTopic>> byName = new HashMap<>();
    private List<Event> events = new ArrayList<>();
    private Function<ResourceName, AsyncResult<Void>> createResponse = n -> Future.failedFuture("Unexpected. ");
    private Function<ResourceName, AsyncResult<Void>> modifyResponse = n -> Future.failedFuture("Unexpected. ");
    private Function<ResourceName, AsyncResult<Void>> deleteResponse = n -> Future.failedFuture("Unexpected. ");
    private Supplier<AsyncResult<List<KafkaTopic>>> listResponse = () -> Future.succeededFuture(new ArrayList(byName.values().stream().filter(ar -> ar.succeeded()).map(ar -> ar.result()).collect(Collectors.toList())));

    public MockK8s setCreateResponse(ResourceName resourceName, Exception exception) {
        Function<ResourceName, AsyncResult<Void>> old = createResponse;
        createResponse = n -> {
            if (resourceName.equals(n)) {
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

    public MockK8s setModifyResponse(ResourceName resourceName, Exception exception) {
        Function<ResourceName, AsyncResult<Void>> old = modifyResponse;
        modifyResponse = n -> {
            if (resourceName.equals(n)) {
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

    public MockK8s setDeleteResponse(ResourceName resourceName, Exception exception) {
        Function<ResourceName, AsyncResult<Void>> old = deleteResponse;
        deleteResponse = n -> {
            if (resourceName.equals(n)) {
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
    public Future<KafkaTopic> createResource(KafkaTopic topicResource) {
        Promise<KafkaTopic> handler = Promise.promise();
        AsyncResult<Void> response = createResponse.apply(new ResourceName(topicResource));
        if (response.succeeded()) {
            AsyncResult<KafkaTopic> old = byName.put(new ResourceName(topicResource), Future.succeededFuture(topicResource));
            if (old != null) {
                handler.handle(Future.failedFuture("resource already existed: " + topicResource.getMetadata().getName()));
                return handler.future();
            }
        }
        if (response.succeeded()) {
            handler.complete(new KafkaTopicBuilder(topicResource).editMetadata().withGeneration(1L).endMetadata().build());
        } else {
            handler.fail(response.cause());
        }
        return handler.future();
    }

    @Override
    public Future<KafkaTopic> updateResource(KafkaTopic topicResource) {
        Promise<KafkaTopic> handler = Promise.promise();
        AsyncResult<Void> response = modifyResponse.apply(new ResourceName(topicResource));
        if (response.succeeded()) {
            AsyncResult<KafkaTopic> old = byName.put(new ResourceName(topicResource), Future.succeededFuture(topicResource));
            if (old == null) {
                handler.handle(Future.failedFuture("resource does not exist, cannot be updated: " + topicResource.getMetadata().getName()));
                return handler.future();
            }
        }
        if (response.succeeded()) {
            Long generation = topicResource.getMetadata().getGeneration();
            handler.complete(new KafkaTopicBuilder(topicResource)
                    .editMetadata()
                        .withGeneration(generation != null ? generation + 1 : 1)
                    .endMetadata()
                .build());
        } else {
            handler.fail(response.cause());
        }
        return handler.future();
    }

    private List<KafkaTopicStatus> statuses = new ArrayList<>();

    public List<KafkaTopicStatus> getStatuses() {
        return Collections.unmodifiableList(statuses);
    }

    @Override
    public Future<KafkaTopic> updateResourceStatus(KafkaTopic topicResource) {
        statuses.add(new KafkaTopicStatusBuilder(topicResource.getStatus()).build());
        Long generation = topicResource.getMetadata().getGeneration();
        return Future.succeededFuture(new KafkaTopicBuilder(topicResource)
                .editMetadata()
                    .withGeneration(generation == null ? 1 : generation + 1)
                .endMetadata()
            .build());
    }

    @Override
    public Future<Void> deleteResource(ResourceName resourceName) {
        Promise<Void> handler = Promise.promise();
        AsyncResult<Void> response = deleteResponse.apply(resourceName);
        if (response.succeeded()) {
            if (byName.remove(resourceName) == null) {
                handler.handle(Future.failedFuture("resource does not exist, cannot be deleted: " + resourceName));
                return handler.future();
            }
        }
        handler.handle(response);
        return handler.future();
    }

    @Override
    public Future<List<KafkaTopic>> listResources() {
        Promise<List<KafkaTopic>> handler = Promise.promise();
        handler.handle(listResponse.get());
        return handler.future();
    }

    public void setListMapsResult(Supplier<AsyncResult<List<KafkaTopic>>> response) {
        this.listResponse = response;
    }

    @Override
    public Future<KafkaTopic> getFromName(ResourceName resourceName) {
        Promise<KafkaTopic> handler = Promise.promise();
        AsyncResult<KafkaTopic> resourceFuture = byName.get(resourceName);
        handler.handle(resourceFuture != null ? resourceFuture : Future.succeededFuture());
        return handler.future();
    }

    @Override
    public Future<Void> createEvent(Event event) {
        Promise<Void> handler = Promise.promise();
        events.add(event);
        handler.handle(Future.succeededFuture());
        return handler.future();
    }

    public void assertExists(VertxTestContext context, ResourceName resourceName) {
        AsyncResult<KafkaTopic> got = byName.get(resourceName);
        context.verify(() -> assertThat(got != null && got.succeeded(), is(true)));
    }

    public void assertContains(VertxTestContext context, KafkaTopic resource) {
        AsyncResult<KafkaTopic> resourceResult = byName.get(new ResourceName(resource));
        context.verify(() -> assertThat(resourceResult.succeeded(), is(true)));
        context.verify(() -> assertThat(resourceResult.result(), is(resource)));
    }

    public void assertNotExists(VertxTestContext context, ResourceName resourceName) {
        context.verify(() -> assertThat(byName.containsKey(resourceName), is(false)));
    }

    public void assertContainsEvent(VertxTestContext context, Predicate<Event> test) {
        for (Event event : events) {
            if (test.test(event)) {
                return;
            }
        }
        context.failNow(new Throwable("Missing event"));
    }

    public void assertNoEvents(VertxTestContext context) {
        context.verify(() -> assertThat(events.isEmpty(), is(true)));
    }

    public void setGetFromNameResponse(ResourceName resourceName, AsyncResult<KafkaTopic> futureResource) {
        this.byName.put(resourceName, futureResource);
    }
}
