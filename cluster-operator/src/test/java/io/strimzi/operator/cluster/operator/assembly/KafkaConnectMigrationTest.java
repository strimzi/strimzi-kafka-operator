/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectMigrationTest {
    private final static String NAME = "my-connect";
    private final static String COMPONENT_NAME = NAME + "-connect";
    private final static String NAMESPACE = "my-namespace";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME);
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final KafkaConnect CONNECT = new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(NAME)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
            .endSpec()
            .build();

    private final static Deployment DEPLOYMENT = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.componentName(NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build();

    private static final KafkaConnectCluster CLUSTER = KafkaConnectCluster.fromCrd(RECONCILIATION, CONNECT, VERSIONS, SHARED_ENV_PROVIDER);

    @Test
    public void testNoMigrationToPodSets(VertxTestContext context)  {
        KafkaConnectMigration migration = new KafkaConnectMigration(
                RECONCILIATION,
                CLUSTER,
                null,
                null,
                1_000L,
                false,
                null,
                null,
                null,
                null,
                null,
                null
        );

        Checkpoint async = context.checkpoint();
        migration.migrateFromDeploymentToStrimziPodSets(null, CLUSTER.generatePodSet(3, null, null, false, null, null, null))
                .onComplete(context.succeeding(v -> context.verify(async::flag)));
    }

    @Test
    public void testMigrationToPodSets(VertxTestContext context)  {
        DeploymentOperator mockDepOps = mock(DeploymentOperator.class);
        StrimziPodSetOperator mockPodSetOps = mock(StrimziPodSetOperator.class);
        PodOperator mockPodOps = mock(PodOperator.class);
        LinkedList<String> events = mockKubernetes(mockDepOps, mockPodSetOps, mockPodOps);

        KafkaConnectMigration migration = new KafkaConnectMigration(
                RECONCILIATION,
                CLUSTER,
                null,
                null,
                1_000L,
                false,
                null,
                null,
                null,
                mockDepOps,
                mockPodSetOps,
                mockPodOps
        );

        Checkpoint async = context.checkpoint();
        migration.migrateFromDeploymentToStrimziPodSets(
                DEPLOYMENT,
                null
        ).onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(events.size(), is(11));

            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-1"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-0"));
            assertThat(events.poll(), is("DEP-SCALE-DOWN-TO-2"));
            assertThat(events.poll(), is("DEP-READINESS-" + COMPONENT_NAME));
            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-2"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-1"));
            assertThat(events.poll(), is("DEP-SCALE-DOWN-TO-1"));
            assertThat(events.poll(), is("DEP-READINESS-" + COMPONENT_NAME));
            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-3"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-2"));
            assertThat(events.poll(), is("DEP-DELETE-" + COMPONENT_NAME));

            async.flag();
        })));
    }

    @Test
    public void testMigrationToPodSetsWithRecreateStrategy(VertxTestContext context)  {
        DeploymentOperator mockDepOps = mock(DeploymentOperator.class);
        StrimziPodSetOperator mockPodSetOps = mock(StrimziPodSetOperator.class);
        PodOperator mockPodOps = mock(PodOperator.class);
        LinkedList<String> events = mockKubernetes(mockDepOps, mockPodSetOps, mockPodOps);

        KafkaConnect connect = new KafkaConnectBuilder(CONNECT)
                .editSpec()
                    .withNewTemplate()
                        .withNewDeployment()
                            .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                        .endDeployment()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster cluster = KafkaConnectCluster.fromCrd(RECONCILIATION, connect, VERSIONS, SHARED_ENV_PROVIDER);

        KafkaConnectMigration migration = new KafkaConnectMigration(
                RECONCILIATION,
                cluster,
                null,
                null,
                1_000L,
                false,
                null,
                null,
                null,
                mockDepOps,
                mockPodSetOps,
                mockPodOps
        );

        Checkpoint async = context.checkpoint();
        migration.migrateFromDeploymentToStrimziPodSets(
                DEPLOYMENT,
                null
        ).onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(events.size(), is(11));

            assertThat(events.poll(), is("DEP-SCALE-DOWN-TO-2"));
            assertThat(events.poll(), is("DEP-READINESS-" + COMPONENT_NAME));
            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-1"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-0"));
            assertThat(events.poll(), is("DEP-SCALE-DOWN-TO-1"));
            assertThat(events.poll(), is("DEP-READINESS-" + COMPONENT_NAME));
            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-2"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-1"));
            assertThat(events.poll(), is("DEP-DELETE-" + COMPONENT_NAME));
            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-3"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-2"));

            async.flag();
        })));
    }

    @Test
    public void testMigrationToPodSetsInTheMiddle(VertxTestContext context)  {
        DeploymentOperator mockDepOps = mock(DeploymentOperator.class);
        StrimziPodSetOperator mockPodSetOps = mock(StrimziPodSetOperator.class);
        PodOperator mockPodOps = mock(PodOperator.class);
        LinkedList<String> events = mockKubernetes(mockDepOps, mockPodSetOps, mockPodOps);

        KafkaConnectMigration migration = new KafkaConnectMigration(
                RECONCILIATION,
                CLUSTER,
                null,
                null,
                1_000L,
                false,
                null,
                null,
                null,
                mockDepOps,
                mockPodSetOps,
                mockPodOps
        );

        Checkpoint async = context.checkpoint();
        migration.migrateFromDeploymentToStrimziPodSets(
                new DeploymentBuilder(DEPLOYMENT).editSpec().withReplicas(2).endSpec().build(),
                CLUSTER.generatePodSet(1, null, null, false, null, null, null)
        ).onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(events.size(), is(7));

            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-2"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-1"));
            assertThat(events.poll(), is("DEP-SCALE-DOWN-TO-1"));
            assertThat(events.poll(), is("DEP-READINESS-" + COMPONENT_NAME));
            assertThat(events.poll(), is("POD-SET-RECONCILE-TO-3"));
            assertThat(events.poll(), is("POD-READINESS-my-connect-connect-2"));
            assertThat(events.poll(), is("DEP-DELETE-" + COMPONENT_NAME));

            async.flag();
        })));
    }

    private static LinkedList<String> mockKubernetes(DeploymentOperator mockDepOps, StrimziPodSetOperator mockPodSetOps, PodOperator mockPodOps)  {
        LinkedList<String> events = new LinkedList<>();

        // Deployments
        when(mockDepOps.scaleDown(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyInt(), anyLong())).thenAnswer(i -> {
            events.add("DEP-SCALE-DOWN-TO-" + i.getArgument(3));
            return Future.succeededFuture();
        });
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenAnswer(i -> {
            events.add("DEP-READINESS-" + i.getArgument(2));
            return Future.succeededFuture();
        });
        when(mockDepOps.deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyBoolean())).thenAnswer(i -> {
            events.add("DEP-DELETE-" + i.getArgument(2));
            return Future.succeededFuture();
        });

        // PodSets
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenAnswer(i -> {
            StrimziPodSet podSet = i.getArgument(3);
            events.add("POD-SET-RECONCILE-TO-" + podSet.getSpec().getPods().size());
            return Future.succeededFuture(ReconcileResult.patched(podSet));
        });

        // Pods
        when(mockPodOps.readiness(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenAnswer(i -> {
            events.add("POD-READINESS-" + i.getArgument(2));
            return Future.succeededFuture();
        });

        return events;
    }
}
