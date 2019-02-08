/*
 * Copyright 2018, Strimzi authors.
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.strimzi.test.TestUtils.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class MockKubeTest<RT extends HasMetadata, LT extends KubernetesResource & KubernetesResourceList,
        DT extends Doneable<RT>> {

    private final Class<RT> cls;
    private final Supplier<RT> factory;
    private final Function<KubernetesClient, MixedOperation<RT, LT, DT, Resource<RT, DT>>> mixedOp;
    private final Consumer<MockKube> init;
    KubernetesClient client;

    @Parameterized.Parameters(name = "{index}: {0}")
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

    public MockKubeTest(Class<RT> cls,
                        Consumer<MockKube> init,
                        Supplier<RT> factory,
                        Function<KubernetesClient, MixedOperation<RT, LT, DT, Resource<RT, DT>>> mixedOp) {
        this.cls = cls;
        this.factory = factory;
        this.mixedOp = mixedOp;
        this.init = init;
    }

    @Before
    public void createClient() {
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

    private RT pod() {
        return factory.get();
    }

    private String expectedResourceExistsMessage(RT resource) {
        return resource.getKind() + " " + resource.getMetadata().getName() + " already exists";
    }

    @Test
    public void podCreateDeleteUnscoped() {
        MyWatcher w = new MyWatcher();
        mixedOp().watch(w);
        RT pod = pod();

        // Create
        mixedOp().create(pod);
        assertEquals(w.lastEvent().action, Watcher.Action.ADDED);
        assertEquals(w.lastEvent().resource, pod);
        try {
            mixedOp().create(pod);
            fail();
        } catch (KubernetesClientException e) {
            assertEquals(expectedResourceExistsMessage(pod), e.getMessage());
        }

        // Delete
        assertTrue(mixedOp().delete(pod));
        assertEquals(w.lastEvent().action, Watcher.Action.DELETED);
        assertEquals(w.lastEvent().resource, pod);
        assertFalse(mixedOp().delete(pod));

        // TODO createOrReplace(), createOrReplaceWithName()
        // TODO delete(List)
    }

    MixedOperation<RT, LT, DT, Resource<RT, DT>> mixedOp() {
        return mixedOp.apply(client);
    }

    @Test
    public void podNameScopedCreateListGetDelete() {
        MyWatcher w = new MyWatcher();
        mixedOp().watch(w);
        RT pod = pod();

        assertEquals(0, mixedOp().list().getItems().size());

        // Create
        mixedOp().withName(pod.getMetadata().getName()).create(pod);
        assertEquals(w.lastEvent().action, Watcher.Action.ADDED);
        assertEquals(w.lastEvent().resource, pod);
        try {
            mixedOp().create(pod);
            fail();
        } catch (KubernetesClientException e) {
            assertEquals(expectedResourceExistsMessage(pod), e.getMessage());
        }

        // List
        List<RT> items = mixedOp().list().getItems();
        assertEquals(1, items.size());
        assertEquals(pod, items.get(0));

        // List with namespace
        items = mixedOp().inNamespace("other").list().getItems();
        // TODO assertEquals(0, items.size());

        // List with labels
        items = mixedOp().withLabel("my-label").list().getItems();
        assertEquals(1, items.size());
        assertEquals(pod, items.get(0));

        items = mixedOp().withLabel("other-label").list().getItems();
        assertEquals(0, items.size());

        items = mixedOp().withLabel("my-label", "foo").list().getItems();
        assertEquals(1, items.size());
        assertEquals(pod, items.get(0));

        items = mixedOp().withLabel("my-label", "bar").list().getItems();
        assertEquals(0, items.size());

        items = mixedOp().withLabels(map("my-label", "foo", "my-other-label", "bar")).list().getItems();
        assertEquals(1, items.size());
        assertEquals(pod, items.get(0));

        items = mixedOp().withLabels(map("my-label", "foo", "my-other-label", "gee")).list().getItems();
        assertEquals(0, items.size());

        // Get
        RT gotResource = mixedOp().withName(pod.getMetadata().getName()).get();
        assertEquals(pod, gotResource);

        // Get with namespace
        gotResource = mixedOp().inNamespace("other").withName(pod.getMetadata().getName()).get();
        // TODO assertNull(gotResource);

        // Delete
        assertTrue(mixedOp().withName(pod.getMetadata().getName()).delete());
        assertEquals(w.lastEvent().action, Watcher.Action.DELETED);
        assertEquals(w.lastEvent().resource, pod);

        items = mixedOp().list().getItems();
        assertEquals(0, items.size());

        gotResource = mixedOp().withName(pod.getMetadata().getName()).get();
        assertNull(gotResource);

        assertFalse(mixedOp().withName(pod.getMetadata().getName()).delete());

        // TODO Delete off a withLabels query, delete off a inNamespace
        // TODO inAnyNamespace()
    }

    // TODO Test Deployment/StatefulSet creation causes ReplicaSet and Pod creation
    // TODO Test Deployment/SS Pod deletion causes new Pod creation
    // TODO Test Pod with VCT causes PVC creation

    // TODO Test other resource types



}
