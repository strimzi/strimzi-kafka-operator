/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerBuilder;
import io.strimzi.systemtest.Environment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

// Deprecation is suppressed because of KafkaMirrorMaker
@SuppressWarnings("deprecation")
public class KafkaMirrorMakerTemplates {

    private KafkaMirrorMakerTemplates() {}

    public static KafkaMirrorMakerBuilder kafkaMirrorMaker(
        String namespaceName,
        String mm1Name,
        String sourceBootstrapServer,
        String targetBootstrapServer,
        String groupId,
        int mirrorMakerReplicas,
        boolean tlsListener
    ) {
        return defaultKafkaMirrorMaker(namespaceName, mm1Name, sourceBootstrapServer, targetBootstrapServer, groupId, mirrorMakerReplicas, tlsListener);
    }

    private static KafkaMirrorMakerBuilder defaultKafkaMirrorMaker(
        String namespaceName,
        String mm1Name,
        String sourceBootstrapServer,
        String targetBootstrapServer,
        String groupId,
        int kafkaMirrorMakerReplicas,
        boolean tlsListener
    ) {
        KafkaMirrorMakerBuilder kmmb = new KafkaMirrorMakerBuilder()
            .withNewMetadata()
                .withName(mm1Name)
                .withNamespace(namespaceName)
            .endMetadata()
            .editSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withNewConsumer()
                    .withBootstrapServers(tlsListener ? KafkaResources.tlsBootstrapAddress(sourceBootstrapServer) : KafkaResources.plainBootstrapAddress(sourceBootstrapServer))
                    .withGroupId(groupId)
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewProducer()
                    .withBootstrapServers(tlsListener ? KafkaResources.tlsBootstrapAddress(targetBootstrapServer) : KafkaResources.plainBootstrapAddress(targetBootstrapServer))
                    .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                .endProducer()
                .withReplicas(kafkaMirrorMakerReplicas)
                .withInclude(".*")
                .withNewInlineLogging()
                    .addToLoggers("mirrormaker.root.logger", "DEBUG")
                .endInlineLogging()
            .endSpec();

        if (!Environment.isSharedMemory()) {
            kmmb.editSpec().withResources(new ResourceRequirementsBuilder()
                // we use such values, because on environments where it is limited to 7Gi, we are unable to deploy
                // Cluster Operator, two Kafka clusters and MirrorMaker/2. Such situation may result in an OOM problem.
                // Using 1Gi is too much and on the other hand 512Mi is causing OOM problem at the start.
                .addToLimits("memory", new Quantity("784Mi"))
                .addToRequests("memory", new Quantity("784Mi"))
                .build());
        }

        return kmmb;

    }
}
