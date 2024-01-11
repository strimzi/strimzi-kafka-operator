/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.model.nodepools.NodeIdAssignment;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaPoolTest {
    private final static String NAMESPACE = "my-namespace";
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private final static String CLUSTER_NAME = "my-cluster";
    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                        .endJbodStorage()
                    .endKafka()
                .endSpec()
                .build();
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("Kafka")
            .withName(CLUSTER_NAME)
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private final static KafkaNodePool POOL = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    @Test
    public void testKafkaPool()  {
        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                POOL,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(kp.isBroker(), is(true));
        assertThat(kp.isController(), is(false));
        assertThat(kp.storage, is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(kp.resources, is(nullValue()));
        assertThat(kp.jvmOptions, is(nullValue()));
        assertThat(kp.gcLoggingEnabled, is(false));
        assertThat(kp.templateContainer, is(nullValue()));
        assertThat(kp.templateInitContainer, is(nullValue()));
        assertThat(kp.templatePod, is(nullValue()));
        assertThat(kp.templatePerBrokerIngress, is(nullValue()));
        assertThat(kp.templatePodSet, is(nullValue()));
        assertThat(kp.templatePerBrokerRoute, is(nullValue()));
        assertThat(kp.templatePerBrokerService, is(nullValue()));
        assertThat(kp.templatePersistentVolumeClaims, is(nullValue()));

        Set<NodeRef> nodes = kp.nodes();
        assertThat(nodes.size(), is(3));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER_NAME + "-pool-10", 10, "pool", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-11", 11, "pool", false, true),
                new NodeRef(CLUSTER_NAME + "-pool-13", 13, "pool", false, true)));

        assertThat(kp.containsNodeId(10), is(true));
        assertThat(kp.containsNodeId(11), is(true));
        assertThat(kp.containsNodeId(12), is(false));
        assertThat(kp.containsNodeId(13), is(true));

        assertThat(kp.nodeRef(11), is(new NodeRef(CLUSTER_NAME + "-pool-11", 11, "pool", false, true)));

        KafkaNodePoolStatus status = kp.generateNodePoolStatus("my-cluster-id");
        assertThat(status.getClusterId(), is("my-cluster-id"));
        assertThat(status.getReplicas(), is(3));
        assertThat(status.getLabelSelector(), is("strimzi.io/cluster=my-cluster,strimzi.io/name=my-cluster-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=pool"));
        assertThat(status.getNodeIds().size(), is(3));
        assertThat(status.getNodeIds(), hasItems(10, 11, 13));
        assertThat(status.getRoles().size(), is(1));
        assertThat(status.getRoles(), hasItems(ProcessRoles.BROKER));
    }

    @Test
    public void testKafkaPoolWithKRaftRoles()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.processRoles, is(Set.of(ProcessRoles.CONTROLLER)));
        assertThat(kp.isBroker(), is(false));
        assertThat(kp.isController(), is(true));

        Set<NodeRef> nodes = kp.nodes();
        assertThat(nodes.size(), is(3));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER_NAME + "-pool-10", 10, "pool", true, false),
                new NodeRef(CLUSTER_NAME + "-pool-11", 11, "pool", true, false),
                new NodeRef(CLUSTER_NAME + "-pool-13", 13, "pool", true, false)));
    }

    @Test
    public void testKafkaPoolWithMixedRoles()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .endSpec()
                .build();

        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.processRoles, is(Set.of(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)));
        assertThat(kp.isBroker(), is(true));
        assertThat(kp.isController(), is(true));

        Set<NodeRef> nodes = kp.nodes();
        assertThat(nodes.size(), is(3));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER_NAME + "-pool-10", 10, "pool", true, true),
                new NodeRef(CLUSTER_NAME + "-pool-11", 11, "pool", true, true),
                new NodeRef(CLUSTER_NAME + "-pool-13", 13, "pool", true, true)));
    }

    @Test
    public void testKafkaPoolConfigureOptionsThroughPoolSpec()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
                    .withNewJvmOptions()
                        .withGcLoggingEnabled()
                        .withXmx("4096m")
                    .endJvmOptions()
                    .withNewTemplate()
                        .withNewKafkaContainer()
                            .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.storage, is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(kp.resources.getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));
        assertThat(kp.gcLoggingEnabled, is(true));
        assertThat(kp.jvmOptions.getXmx(), is("4096m"));
        assertThat(kp.templateContainer.getEnv(), is(List.of(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())));
        assertThat(kp.templateInitContainer, is(nullValue()));
        assertThat(kp.templatePod, is(nullValue()));
        assertThat(kp.templatePerBrokerIngress, is(nullValue()));
        assertThat(kp.templatePodSet, is(nullValue()));
        assertThat(kp.templatePerBrokerRoute, is(nullValue()));
        assertThat(kp.templatePerBrokerService, is(nullValue()));
        assertThat(kp.templatePersistentVolumeClaims, is(nullValue()));
    }

    @Test
    public void testKafkaPoolConfigureOptionsThroughKafka()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
                        .withNewJvmOptions()
                            .withGcLoggingEnabled()
                            .withXmx("4096m")
                        .endJvmOptions()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                POOL,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.storage, is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(kp.resources.getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));
        assertThat(kp.gcLoggingEnabled, is(true));
        assertThat(kp.jvmOptions.getXmx(), is("4096m"));
        assertThat(kp.templateContainer.getEnv(), is(List.of(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())));
        assertThat(kp.templateInitContainer, is(nullValue()));
        assertThat(kp.templatePod, is(nullValue()));
        assertThat(kp.templatePerBrokerIngress, is(nullValue()));
        assertThat(kp.templatePodSet, is(nullValue()));
        assertThat(kp.templatePerBrokerRoute, is(nullValue()));
        assertThat(kp.templatePerBrokerService, is(nullValue()));
        assertThat(kp.templatePersistentVolumeClaims, is(nullValue()));
    }

    @Test
    public void testKafkaPoolConfigureOptionsConflict()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
                    .withNewJvmOptions()
                        .withGcLoggingEnabled()
                        .withXmx("4096m")
                    .endJvmOptions()
                    .withNewTemplate()
                        .withNewKafkaContainer()
                            .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("6"), "memory", new Quantity("20Gi"))).build())
                        .withNewJvmOptions()
                            .withGcLoggingEnabled()
                            .withXmx("8192m")
                        .endJvmOptions()
                        .withNewTemplate()
                            .withNewInitContainer()
                                .addToEnv(new ContainerEnvVarBuilder().withName("MY_INIT_ENV_VAR").withValue("my-init-env-var-value").build())
                            .endInitContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.storage, is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(kp.resources.getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));
        assertThat(kp.gcLoggingEnabled, is(true));
        assertThat(kp.jvmOptions.getXmx(), is("4096m"));
        assertThat(kp.templateContainer.getEnv(), is(List.of(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())));
        assertThat(kp.templateInitContainer, is(nullValue()));
        assertThat(kp.templatePod, is(nullValue()));
        assertThat(kp.templatePerBrokerIngress, is(nullValue()));
        assertThat(kp.templatePodSet, is(nullValue()));
        assertThat(kp.templatePerBrokerRoute, is(nullValue()));
        assertThat(kp.templatePerBrokerService, is(nullValue()));
        assertThat(kp.templatePersistentVolumeClaims, is(nullValue()));
    }

    @Test
    public void testKafkaPoolConfigureOptionsMixed()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withNewTemplate()
                        .withNewKafkaContainer()
                            .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("6"), "memory", new Quantity("20Gi"))).build())
                        .withNewJvmOptions()
                            .withGcLoggingEnabled()
                            .withXmx("8192m")
                        .endJvmOptions()
                        .withNewTemplate()
                            .withNewInitContainer()
                                .addToEnv(new ContainerEnvVarBuilder().withName("MY_INIT_ENV_VAR").withValue("my-init-env-var-value").build())
                            .endInitContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaPool kp = KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        );

        assertThat(kp, is(notNullValue()));
        assertThat(kp.componentName, is(CLUSTER_NAME + "-pool"));
        assertThat(kp.storage, is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(kp.resources.getRequests(), is(Map.of("cpu", new Quantity("6"), "memory", new Quantity("20Gi"))));
        assertThat(kp.gcLoggingEnabled, is(true));
        assertThat(kp.jvmOptions.getXmx(), is("8192m"));
        assertThat(kp.templateContainer.getEnv(), is(List.of(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())));
        assertThat(kp.templateInitContainer, is(nullValue()));
        assertThat(kp.templatePod, is(nullValue()));
        assertThat(kp.templatePerBrokerIngress, is(nullValue()));
        assertThat(kp.templatePodSet, is(nullValue()));
        assertThat(kp.templatePerBrokerRoute, is(nullValue()));
        assertThat(kp.templatePerBrokerService, is(nullValue()));
        assertThat(kp.templatePersistentVolumeClaims, is(nullValue()));
    }

    @Test
    public void testResourceValidation()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("-16Gi")))
                            .withLimits(Map.of("cpu", new Quantity("2"), "memory", new Quantity("16Gi")))
                            .build())
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        ));

        assertThat(ex.getMessage(), containsString("KafkaNodePool.spec.resources cpu request must be <= limit"));
        assertThat(ex.getMessage(), containsString("KafkaNodePool.spec.resources memory request must be > zero"));
    }

    @Test
    public void testStorageValidation()  {
        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL)
                .editSpec()
                    .withNewJbodStorage()
                    .endJbodStorage()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaPool.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                KAFKA,
                pool,
                new NodeIdAssignment(Set.of(10, 11, 13), Set.of(10, 11, 13), Set.of(), Set.of(), Set.of()),
                null,
                OWNER_REFERENCE,
                SHARED_ENV_PROVIDER
        ));

        assertThat(ex.getMessage(), containsString("JbodStorage needs to contain at least one volume (KafkaNodePool.spec.storage"));
    }
}
