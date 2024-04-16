/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LeaderElectionManagerIT {
    private final static String NAMESPACE = "my-le-namespace";
    private final static String LEASE_NAME = "my-lease";

    private static KubernetesClient client;

    @BeforeAll
    static void setupEnvironment() {
        client = new KubernetesClientBuilder().build();
        client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build()).create();
        client.namespaces().withName(NAMESPACE).waitUntilCondition(ns -> ns.getStatus() != null && "Active".equals(ns.getStatus().getPhase()), 30_000, TimeUnit.MILLISECONDS);
    }

    @AfterAll
    static void teardownEnvironment() {
        client.namespaces().withName(NAMESPACE).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
        client.namespaces().withName(NAMESPACE).waitUntilCondition(Objects::isNull, 30_000, TimeUnit.MILLISECONDS);
        client.close();
    }

    @Test
    public void testLeaderElectionManager() throws InterruptedException {
        CountDownLatch le1Leader = new CountDownLatch(1);
        CountDownLatch le1NotLeader = new CountDownLatch(1);
        CountDownLatch le2Leader = new CountDownLatch(1);
        CountDownLatch le2NotLeader = new CountDownLatch(1);

        LeaderElectionManager le1 = createLeaderElectionManager("le-1", le1Leader::countDown, i -> le1NotLeader.countDown());
        LeaderElectionManager le2 = createLeaderElectionManager("le-2", le2Leader::countDown, i -> le2NotLeader.countDown());

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

        // Stop the second member => the leadership should be released
        le2.stop();
        le2NotLeader.await();
        assertThat(getLease().getSpec().getHolderIdentity(), is(""));
    }

    private LeaderElectionManager createLeaderElectionManager(String identity, Runnable startLeadershipCallback, Consumer<Boolean> stopLeadershipCallback)   {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), LEASE_NAME);
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), NAMESPACE);
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), identity);
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_DURATION_MS.key(), "1000");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_RENEW_DEADLINE_MS.key(), "800");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_RETRY_PERIOD_MS.key(), "200");

        return new LeaderElectionManager(
                client, LeaderElectionManagerConfig.fromMap(envVars),
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
