/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorPodSetWithoutSSATest extends KafkaAssemblyOperatorPodSetTest {
    @Override
    Kafka initKafka() {
        return new KafkaBuilder()
            .withNewMetadata()
            .withName(clusterName)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withNewKafka()
            .withReplicas(3)
            .withListeners(new GenericKafkaListenerBuilder()
                    .withName("plain")
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(false)
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
