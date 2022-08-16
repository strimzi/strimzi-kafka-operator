/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LeaderElectionManagerIT {
    private final static String NAMESPACE = "my-le-namespace";
    private final static String LEASE_NAME = "my-lease";

    private final static KubeClusterResource CLUSTER = KubeClusterResource.getInstance();

    private static KubernetesClient client;

    @BeforeAll
    static void setupEnvironment() {
        CLUSTER.createNamespace(NAMESPACE);
        client = new KubernetesClientBuilder().build();
    }

    @AfterAll
    static void teardownEnvironment() {
        CLUSTER.deleteNamespaces();
    }

    @Test
    public void testLeaderElectionManager() throws InterruptedException {
        CountDownLatch le1Leader = new CountDownLatch(1);
        CountDownLatch le1NotLeader = new CountDownLatch(1);
        CountDownLatch le2Leader = new CountDownLatch(1);
        CountDownLatch le2NotLeader = new CountDownLatch(1);

        LeaderElectionManager le1 = createLeaderElectionManager("le-1", le1Leader::countDown, le1NotLeader::countDown);
        LeaderElectionManager le2 = createLeaderElectionManager("le-2", le2Leader::countDown, le2NotLeader::countDown);

        // Start the first member => it should become a leader
        le1.start();
        le1Leader.await();
        assertThat(getLease().getSpec().getHolderIdentity(), is("le-1"));

        // Start the second member => leadership should not change
        le2.start();
        assertThat(getLease().getSpec().getHolderIdentity(), is("le-1"));

        // Stop the first member => leadership should change
        le1.stop();
        le1NotLeader.await();
        le2Leader.await();
        assertThat(getLease().getSpec().getHolderIdentity(), is("le-2"));

        // Stop the second member => the leadership in the lease resource will stay as it was
        le2.stop();
        le2NotLeader.await();
        assertThat(getLease().getSpec().getHolderIdentity(), is("le-2"));
    }

    private LeaderElectionManager createLeaderElectionManager(String identity, Runnable startLeadershipCallback, Runnable stopLeadershipCallback)   {
        return new LeaderElectionManager(
                client, new LeaderElectionManagerConfig(LEASE_NAME, NAMESPACE, identity, Duration.ofMillis(1_000L), Duration.ofMillis(800L), Duration.ofMillis(200L)),
                startLeadershipCallback,
                stopLeadershipCallback,
                s -> {
                    // Do nothing
                });
    }

    private Lease getLease()    {
        return client.leases().inNamespace(NAMESPACE).withName(LEASE_NAME).get();
    }
}
