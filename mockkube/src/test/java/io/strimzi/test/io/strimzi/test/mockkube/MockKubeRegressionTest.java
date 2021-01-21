/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.fail;

public class MockKubeRegressionTest {

    private KubernetesClient client;

    @BeforeEach
    public void before() {
        client = new MockKube().build();
    }

    @Test
    public void testStatefulSetCreationAndDeletion() {
        StatefulSet sts = new StatefulSetBuilder()
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
                .endSpec().build();
        client.apps().statefulSets().inNamespace("ns").withName("foo").create(sts);

        List<Pod> ns = client.pods().inNamespace("ns").list().getItems();
        assertThat(ns, hasSize(3));

        AtomicBoolean deleted = new AtomicBoolean(false);
        AtomicBoolean recreated = new AtomicBoolean(false);
        Watch watch = client.pods().inNamespace("ns").withName(ns.get(0).getMetadata().getName()).watch(new Watcher<Pod>() {
            @Override
            public void eventReceived(Action action, Pod resource) {
                if (action == Action.DELETED) {
                    if (deleted.getAndSet(true)) {
                        fail("Deleted twice");
                    }
                } else if (action == Action.ADDED) {
                    if (!deleted.get()) {
                        fail("Created before deleted");
                    }
                    if (recreated.getAndSet(true)) {
                        fail("Recreated twice");
                    }
                }
            }

            @Override
            public void onClose(WatcherException cause) {

            }
        });
        client.pods().inNamespace("ns").withName(ns.get(0).getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        assertThat(deleted.get(), is(true));
        assertThat(recreated.get(), is(true));
        watch.close();

        ns = client.pods().inNamespace("ns").list().getItems();
        assertThat(ns, hasSize(3));

        client.apps().statefulSets().inNamespace("ns").withName("foo").withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

    }
}
