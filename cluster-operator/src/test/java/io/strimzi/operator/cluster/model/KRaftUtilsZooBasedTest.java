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
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KRaftUtilsZooBasedTest {
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
}
