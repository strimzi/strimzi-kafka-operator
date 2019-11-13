/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.strimzi.test.TestUtils.map;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class MockKubeTest<RT extends HasMetadata, LT extends KubernetesResource & KubernetesResourceList,
        DT extends Doneable<RT>> {

    KubernetesClient client;

    @SuppressWarnings("unchecked")
    public static Iterable<Object[]> parameters() {
        return Arrays.<Object[]>asList(
                new Object[]{Pod.class,
                    (Consumer<MockKube>) mockKube -> {
                    },
                    (Supplier<HasMetadata>) () ->
                        new PodBuilder()
                            .withNewMetadata()
                                .withName("my-pod")
                                .withNamespace("my-project")
                                .addToLabels("my-label", "foo")
                                .addToLabels("my-other-label", "bar")
                            .endMetadata()
                        .build(),
                    (Function<KubernetesClient, MixedOperation>) client -> client.pods()
                },
                new Object[]{KafkaTopic.class,
                    (Consumer<MockKube>) mockKube -> {
                        mockKube.withCustomResourceDefinition(Crds.topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
                    },
                    (Supplier<HasMetadata>) () ->
                        new KafkaTopicBuilder()
                            .withNewMetadata()
                                .withName("my-topic")
                                .withNamespace("my-project")
                                .addToLabels("my-label", "foo")
                                .addToLabels("my-other-label", "bar")
                            .endMetadata()
                        .build(),
                    (Function<KubernetesClient, MixedOperation>) client -> Crds.topicOperation(client)
                    }
                );
    }

    public void createClient(Consumer<MockKube> init) throws MalformedURLException {
        MockKube mockKube = new MockKube();
        init.accept(mockKube);
        client = mockKube.build();
    }

    private static class MyWatcher<T> implements Watcher<T> {

        static class Event<T> {
            private final Action action;
            private final T resource;

            Event(Action action, T resource) {
                this.action = action;
                this.resource = resource;
            }
        }

        public List<Event<T>> events = new ArrayList<>();
        public boolean closed = false;

        public Event<T> lastEvent() {
            if (events.isEmpty()) {
                return null;
            } else {
                return events.get(events.size() - 1);
            }
        }

        @Override
        public void eventReceived(Action action, T resource) {
            if (!closed) {
                events.add(new Event<>(action, resource));
            } else {
                throw new AssertionError("Event received by closed watcher");
            }
        }

        @Override
        public void onClose(KubernetesClientException cause) {
            this.closed = true;
        }
    }

    private String expectedResourceExistsMessage(RT resource) {
        return resource.getKind() + " " + resource.getMetadata().getName() + " already exists";
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("parameters")
    public void podCreateDeleteUnscoped(Class<RT> cls,
                                        Consumer<MockKube> init,
                                        Supplier<RT> factory,
                                        Function<KubernetesClient, MixedOperation<RT, LT, DT, Resource<RT, DT>>> mixedOp) throws MalformedURLException {
        createClient(init);
        MyWatcher w = new MyWatcher();
        mixedOp.apply(client).watch(w);
        RT pod = factory.get();

        // Create
        mixedOp.apply(client).create(pod);
        assertThat(w.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(w.lastEvent().resource, is(pod));
        try {
            mixedOp.apply(client).create(pod);
            fail();
        } catch (KubernetesClientException e) {
            assertThat(e.getMessage(), is(expectedResourceExistsMessage(pod)));
        }

        // Delete
        assertThat(mixedOp.apply(client).delete(pod), is(true));
        assertThat(w.lastEvent().action, is(Watcher.Action.DELETED));
        // Compare ignoring resource version
        RT resource = (RT) w.lastEvent().resource;
        resource.getMetadata().setResourceVersion(null);
        assertEquals(resource, pod);
        assertThat(mixedOp.apply(client).delete(pod), is(false));

        // TODO createOrReplace(), createOrReplaceWithName()
        // TODO delete(List)
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("parameters")
    public void podNameScopedCreateListGetDelete(Class<RT> cls,
                                                 Consumer<MockKube> init,
                                                 Supplier<RT> factory,
                                                 Function<KubernetesClient, MixedOperation<RT, LT, DT, Resource<RT, DT>>> mixedOp) throws MalformedURLException {
        createClient(init);
        MyWatcher w = new MyWatcher();
        mixedOp.apply(client).watch(w);
        RT pod = factory.get();

        assertThat(mixedOp.apply(client).list().getItems().size(), is(0));

        // Create
        mixedOp.apply(client).withName(pod.getMetadata().getName()).create(pod);
        assertThat(w.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(w.lastEvent().resource, is(pod));
        try {
            mixedOp.apply(client).create(pod);
            fail();
        } catch (KubernetesClientException e) {
            assertThat(e.getMessage(), is(expectedResourceExistsMessage(pod)));
        }

        // List
        List<RT> items = mixedOp.apply(client).list().getItems();
        assertThat(items.size(), is(1));
        RT item = items.get(0);
        item.getMetadata().setResourceVersion(null);
        assertThat(item, is(pod));

        // List with namespace
        items = mixedOp.apply(client).inNamespace("other").list().getItems();
        // TODO assertEquals(0, items.size());

        // List with labels
        items = mixedOp.apply(client).withLabel("my-label").list().getItems();
        assertThat(items.size(), is(1));
        RT actual = items.get(0);
        actual.getMetadata().setResourceVersion(null);
        assertThat(actual, is(pod));

        items = mixedOp.apply(client).withLabel("other-label").list().getItems();
        assertThat(items.size(), is(0));

        items = mixedOp.apply(client).withLabel("my-label", "foo").list().getItems();
        assertThat(items.size(), is(1));
        RT actual1 = items.get(0);
        actual1.getMetadata().setResourceVersion(null);
        assertThat(actual1, is(pod));

        items = mixedOp.apply(client).withLabel("my-label", "bar").list().getItems();
        assertThat(items.size(), is(0));

        items = mixedOp.apply(client).withLabels(map("my-label", "foo", "my-other-label", "bar")).list().getItems();
        assertThat(items.size(), is(1));
        RT actual2 = items.get(0);
        actual2.getMetadata().setResourceVersion(null);
        assertThat(actual2, is(pod));

        items = mixedOp.apply(client).withLabels(map("my-label", "foo", "my-other-label", "gee")).list().getItems();
        assertThat(items.size(), is(0));

        // Get
        RT gotResource = mixedOp.apply(client).withName(pod.getMetadata().getName()).get();
        gotResource.getMetadata().setResourceVersion(null);
        assertThat(gotResource, is(pod));

        // Get with namespace
        gotResource = mixedOp.apply(client).inNamespace("other").withName(pod.getMetadata().getName()).get();
        // TODO assertNull(gotResource);

        // Delete
        assertThat(mixedOp.apply(client).withName(pod.getMetadata().getName()).delete(), is(true));
        assertThat(w.lastEvent().action, is(Watcher.Action.DELETED));
        RT resource = (RT) w.lastEvent().resource;
        resource.getMetadata().setResourceVersion(null);
        assertThat(resource, is(pod));

        items = mixedOp.apply(client).list().getItems();
        assertThat(items.size(), is(0));

        gotResource = mixedOp.apply(client).withName(pod.getMetadata().getName()).get();
        assertThat(gotResource, is(nullValue()));

        assertThat(mixedOp.apply(client).withName(pod.getMetadata().getName()).delete(), is(false));

        // TODO Delete off a withLabels query, delete off a inNamespace
        // TODO inAnyNamespace()
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("parameters")
    public void watches(Class<RT> cls,
                        Consumer<MockKube> init,
                        Supplier<RT> factory,
                        Function<KubernetesClient, MixedOperation<RT, LT, DT, Resource<RT, DT>>> mixedOp) throws MalformedURLException {
        createClient(init);
        RT pod = factory.get();

        MyWatcher all = new MyWatcher();
        MyWatcher namedMyPod = new MyWatcher();
        MyWatcher namedYourPod = new MyWatcher();
        MyWatcher hasMyLabel = new MyWatcher();
        MyWatcher hasYourLabel = new MyWatcher();
        MyWatcher hasMyLabelFoo = new MyWatcher();
        MyWatcher hasMyLabelBar = new MyWatcher();
        MyWatcher hasBothMyLabels = new MyWatcher();
        MyWatcher hasOnlyOneOfMyLabels = new MyWatcher();
        mixedOp.apply(client).watch(all);
        mixedOp.apply(client).withName(pod.getMetadata().getName()).watch(namedMyPod);
        mixedOp.apply(client).withName("your-pod").watch(namedYourPod);
        mixedOp.apply(client).withLabel("my-label").watch(hasMyLabel);
        mixedOp.apply(client).withLabel("your-label").watch(hasYourLabel);
        mixedOp.apply(client).withLabel("my-label", "foo").watch(hasMyLabelFoo);
        mixedOp.apply(client).withLabel("my-label", "bar").watch(hasMyLabelBar);
        mixedOp.apply(client).withLabels(map("my-label", "foo", "my-other-label", "bar")).watch(hasBothMyLabels);
        mixedOp.apply(client).withLabels(map("my-label", "foo", "your-label", "bar")).watch(hasOnlyOneOfMyLabels);

        mixedOp.apply(client).withName(pod.getMetadata().getName()).create(pod);

        assertThat(all.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(all.lastEvent().resource, is(pod));
        assertThat(namedMyPod.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(namedMyPod.lastEvent().resource, is(pod));
        assertThat(namedYourPod.events.isEmpty(), is(true));
        assertThat(hasMyLabel.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(hasMyLabel.lastEvent().resource, is(pod));
        assertThat(hasYourLabel.events.isEmpty(), is(true));
        assertThat(hasMyLabelFoo.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(hasMyLabelFoo.lastEvent().resource, is(pod));
        assertThat(hasMyLabelBar.events.isEmpty(), is(true));
        assertThat(hasBothMyLabels.lastEvent().action, is(Watcher.Action.ADDED));
        assertThat(hasBothMyLabels.lastEvent().resource, is(pod));
        assertThat(hasOnlyOneOfMyLabels.events.isEmpty(), is(true));
    }

    // TODO Test Deployment/StatefulSet creation causes ReplicaSet and Pod creation
    // TODO Test Deployment/SS Pod deletion causes new Pod creation
    // TODO Test Pod with VCT causes PVC creation

    // TODO Test other resource types
}
