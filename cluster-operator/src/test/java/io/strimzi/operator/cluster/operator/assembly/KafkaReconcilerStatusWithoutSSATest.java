/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.ResourceUtils;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerStatusWithoutSSATest extends KafkaReconcilerStatusTest {
    @Override
    ClusterOperatorConfig getClusterOperatorConfig() {
        return ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
    }

    @Override
    Kafka getKafkaCrd() {
        return new KafkaBuilder()
            .withNewMetadata()
            .withName(CLUSTER_NAME)
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewKafka()
            .withReplicas(3)
            .withListeners(new GenericKafkaListenerBuilder()
                    .withName("tls")
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(true)
                    .build())
            .withNewEphemeralStorage()
            .endEphemeralStorage()
            .endKafka()
            .withNewZookeeper()
            .withReplicas(3)
            .withNewEphemeralStorage()
            .endEphemeralStorage()
            .endZookeeper()
            .endSpec()
            .build();
    }
}
