/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.api.kafka.model.kafka.KafkaSpecBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KRaftUtilsTest {
    @ParallelTest
    public void testValidKafka() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8080")
                    .endKafkaAuthorizationOpa()
                .endKafka()
                .build();

        assertDoesNotThrow(() -> KRaftUtils.validateKafkaCrForKRaft(spec, false));
    }

    @ParallelTest
    public void testInvalidKafka() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build())
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                                new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                    .endJbodStorage()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateKafkaCrForKRaft(spec, false));

        assertThat(ex.getMessage(), is("Kafka configuration is not valid: [Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled.]"));
    }

    @ParallelTest
    public void testNoEntityOperator() {
        Set<String> errors = new HashSet<>(0);
        KRaftUtils.validateEntityOperatorSpec(errors, null, false);

        assertThat(errors, is(Collections.emptySet()));
    }

    @ParallelTest
    public void testEnabledOnlyUserOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, false);
        assertThat(errors, is(Collections.emptySet()));
    }

    @ParallelTest
    public void testEnabledEntityOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, false);

        assertThat(errors, is(Set.of("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled.")));
    }

    @ParallelTest
    public void testEnabledUnidirectionalTopicOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, true);

        assertThat(errors.size(), is(0));
    }

    @ParallelTest
    public void testEnabledEntityOperatorOnlyTopicOperator() {
        Set<String> errors = new HashSet<>(0);
        EntityOperatorSpec eo = new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();

        KRaftUtils.validateEntityOperatorSpec(errors, eo, false);

        assertThat(errors, is(Set.of("Only Unidirectional Topic Operator is supported when the UseKRaft feature gate is enabled.")));
    }

    @ParallelTest
    public void testKRaftMetadataVersionValidation()    {
        // Valid values
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.6"));
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.6-IV2"));

        // Minimum supported versions
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.3"));
        assertDoesNotThrow(() -> KRaftUtils.validateMetadataVersion("3.3-IV0"));

        // Invalid Values
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3.6-IV9"));
        assertThat(e.getMessage(), containsString("Metadata version 3.6-IV9 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3"));
        assertThat(e.getMessage(), containsString("Metadata version 3 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3.2"));
        assertThat(e.getMessage(), containsString("The oldest supported metadata version is 3.3-IV0"));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateMetadataVersion("3.2-IV0"));
        assertThat(e.getMessage(), containsString("The oldest supported metadata version is 3.3-IV0"));
    }

    @ParallelTest
    public void testValidZooBasedCluster() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewZookeeper()
                    .withReplicas(3)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endZookeeper()
                .withNewKafka()
                    .withReplicas(3)
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8080")
                    .endKafkaAuthorizationOpa()
                .endKafka()
                .build();

        assertDoesNotThrow(() -> KRaftUtils.validateKafkaCrForZooKeeper(spec, false));
    }

    @ParallelTest
    public void testValidZooBasedClusterWithNodePools() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewZookeeper()
                    .withReplicas(3)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endZookeeper()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8080")
                    .endKafkaAuthorizationOpa()
                .endKafka()
                .build();

        assertDoesNotThrow(() -> KRaftUtils.validateKafkaCrForZooKeeper(spec, true));
    }

    @ParallelTest
    public void testZooBasedClusterWithMissingZooSection() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewKafka()
                    .withReplicas(3)
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8080")
                    .endKafkaAuthorizationOpa()
                .endKafka()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateKafkaCrForZooKeeper(spec, false));
        assertThat(e.getMessage(), containsString("The .spec.zookeeper section of the Kafka custom resource is missing. This section is required for a ZooKeeper-based cluster."));
    }

    @ParallelTest
    public void testZooBasedClusterWithMissingReplicasAndStorage() {
        KafkaSpec spec = new KafkaSpecBuilder()
                .withNewZookeeper()
                    .withReplicas(3)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endZookeeper()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("listener")
                            .withPort(9092)
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                    .withNewKafkaAuthorizationOpa()
                        .withUrl("http://opa:8080")
                    .endKafkaAuthorizationOpa()
                .endKafka()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateKafkaCrForZooKeeper(spec, false));
        assertThat(e.getMessage(), containsString("The .spec.kafka.replicas property of the Kafka custom resource is missing. This property is required for a ZooKeeper-based Kafka cluster that is not using Node Pools."));
        assertThat(e.getMessage(), containsString("The .spec.kafka.storage section of the Kafka custom resource is missing. This section is required for a ZooKeeper-based Kafka cluster that is not using Node Pools."));
    }

    @ParallelTest
    public void testKRaftWarnings() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("listener")
                                .withPort(9092)
                                .withTls(true)
                                .withType(KafkaListenerType.INTERNAL)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withNewKafkaAuthorizationOpa()
                            .withUrl("http://opa:8080")
                        .endKafkaAuthorizationOpa()
                    .endKafka()
                .endSpec()
                .build();

        KafkaStatus status = new KafkaStatus();
        KRaftUtils.kraftWarnings(kafka, status);

        assertThat(status.getConditions().size(), is(3));

        Condition condition = status.getConditions().stream().filter(c -> "UnusedZooKeeperConfiguration".equals(c.getReason())).findFirst().orElseThrow();
        assertThat(condition.getMessage(), is("The .spec.zookeeper section in the Kafka custom resource is ignored in KRaft mode and should be removed from the custom resource."));
        assertThat(condition.getType(), is("Warning"));
        assertThat(condition.getStatus(), is("True"));

        condition = status.getConditions().stream().filter(c -> "UnusedReplicasConfiguration".equals(c.getReason())).findFirst().orElseThrow();
        assertThat(condition.getMessage(), is("The .spec.kafka.replicas property in the Kafka custom resource is ignored when node pools are used and should be removed from the custom resource."));
        assertThat(condition.getType(), is("Warning"));
        assertThat(condition.getStatus(), is("True"));

        condition = status.getConditions().stream().filter(c -> "UnusedStorageConfiguration".equals(c.getReason())).findFirst().orElseThrow();
        assertThat(condition.getMessage(), is("The .spec.kafka.storage section in the Kafka custom resource is ignored when node pools are used and should be removed from the custom resource."));
        assertThat(condition.getType(), is("Warning"));
        assertThat(condition.getStatus(), is("True"));
    }

    @ParallelTest
    public void testZooKeeperWarnings() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("listener")
                                .withPort(9092)
                                .withTls(true)
                                .withType(KafkaListenerType.INTERNAL)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withNewKafkaAuthorizationOpa()
                            .withUrl("http://opa:8080")
                        .endKafkaAuthorizationOpa()
                    .endKafka()
                .endSpec()
                .build();

        KafkaStatus status = new KafkaStatus();
        KRaftUtils.nodePoolWarnings(kafka, status);

        assertThat(status.getConditions().size(), is(2));

        Condition condition = status.getConditions().stream().filter(c -> "UnusedReplicasConfiguration".equals(c.getReason())).findFirst().orElseThrow();
        assertThat(condition.getMessage(), is("The .spec.kafka.replicas property in the Kafka custom resource is ignored when node pools are used and should be removed from the custom resource."));
        assertThat(condition.getType(), is("Warning"));
        assertThat(condition.getStatus(), is("True"));

        condition = status.getConditions().stream().filter(c -> "UnusedStorageConfiguration".equals(c.getReason())).findFirst().orElseThrow();
        assertThat(condition.getMessage(), is("The .spec.kafka.storage section in the Kafka custom resource is ignored when node pools are used and should be removed from the custom resource."));
        assertThat(condition.getType(), is("Warning"));
        assertThat(condition.getStatus(), is("True"));
    }

    @ParallelTest
    public void testsVersionsForKRaftMigrationValidation() {
        // Valid values
        assertDoesNotThrow(() -> KRaftUtils.validateVersionsForKRaftMigration("3.6.1", "3.6-IV2", "3.6", "3.6"));

        // Invalid Values
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateVersionsForKRaftMigration("3.6.1", "3.6-IV2", "3.5", "3.5"));
        assertThat(e.getMessage(), containsString("Migration cannot be performed with Kafka version 3.6-IV2, metadata version 3.6-IV2, inter.broker.protocol.version 3.5-IV2, log.message.format.version 3.5-IV2."));

        e = assertThrows(InvalidResourceException.class, () -> KRaftUtils.validateVersionsForKRaftMigration("3.6.1", "3.5-IV2", "3.6", "3.6"));
        assertThat(e.getMessage(), containsString("Migration cannot be performed with Kafka version 3.6-IV2, metadata version 3.5-IV2, inter.broker.protocol.version 3.6-IV2, log.message.format.version 3.6-IV2."));
    }
}
