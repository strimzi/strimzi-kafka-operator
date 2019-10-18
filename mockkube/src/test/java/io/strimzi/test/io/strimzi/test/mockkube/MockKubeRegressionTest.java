/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.test.mockkube.MockKube;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class MockKubeRegressionTest {

    private KubernetesClient client;

    @Before
    public void before() {
        client = new MockKube().build();
    }

    @Test
    public void test1() {
        client.apps().statefulSets().inNamespace("ns").withName("foo").createNew()
                .withNewMetadata()
                    .withName("foo")
                    .withNamespace("ns")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewTemplate()
                        .withNewMetadata().endMetadata()
                        .withNewSpec().endSpec()
                    .endTemplate()
                .endSpec()
            .done();

        List<Pod> ns = client.pods().inNamespace("ns").list().getItems();
        assertEquals(3, ns.size());

        AtomicBoolean deleted = new AtomicBoolean(false);
        AtomicBoolean recreated = new AtomicBoolean(false);
        Watch watch = client.pods().inNamespace("ns").withName(ns.get(0).getMetadata().getName()).watch(new Watcher<Pod>() {
            @Override
            public void eventReceived(Action action, Pod resource) {
                if (action == Action.DELETED) {
                    if (deleted.getAndSet(true)) {
                        Assert.fail("Deleted twice");
                    }
                } else if (action == Action.ADDED) {
                    if (!deleted.get()) {
                        Assert.fail("Created before deleted");
                    }
                    if (recreated.getAndSet(true)) {
                        Assert.fail("Recreated twice");
                    }
                }
            }

            @Override
            public void onClose(KubernetesClientException cause) {

            }
        });
        client.pods().inNamespace("ns").withName(ns.get(0).getMetadata().getName()).delete();

        Assert.assertTrue(deleted.get());
        Assert.assertTrue(recreated.get());
        watch.close();

        ns = client.pods().inNamespace("ns").list().getItems();
        assertEquals(3, ns.size());

        client.apps().statefulSets().inNamespace("ns").withName("foo").delete();

    }
}
