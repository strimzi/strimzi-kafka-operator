/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;

final class KafkaClusterTestUtils {
    static final String NAMESPACE = "test";
    static final String CLUSTER = "foo";

    private KafkaClusterTestUtils() { }

    static Kafka createKafka() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                        .withName("plain")
                                        .withPort(9092)
                                        .withType(KafkaListenerType.INTERNAL)
                                        .withTls(false)
                                        .build(),
                                new GenericKafkaListenerBuilder()
                                        .withName("tls")
                                        .withPort(9093)
                                        .withType(KafkaListenerType.INTERNAL)
                                        .withTls(true)
                                        .build())
                    .endKafka()
                .endSpec()
                .build();
    }

    static KafkaNodePool createControllerPool() {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("controllers")
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();
    }

    static KafkaNodePool createMixedPool() {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("mixed")
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(2)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .endSpec()
                .build();
    }

    static KafkaNodePool createBrokerPool() {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("brokers")
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
    }
}
