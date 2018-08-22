/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class Resources {

    private static final Logger LOGGER = LogManager.getLogger(Resources.class);

    private final NamespacedKubernetesClient client;

    Resources(NamespacedKubernetesClient client) {
        this.client = client;
    }

    private NamespacedKubernetesClient client() {
        return client;
    }

    private MixedOperation<Kafka, KafkaAssemblyList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafka() {
        return client()
                .customResources(Crds.kafka(),
                        Kafka.class, KafkaAssemblyList.class, DoneableKafka.class);
    }

    private MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> kafkaTopic() {
        return client()
                .customResources(Crds.topic(),
                        KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    private MixedOperation<KafkaUser, KafkaUserList, DoneableKafkaUser, Resource<KafkaUser, DoneableKafkaUser>> kafkaUser() {
        return client()
                .customResources(Crds.kafkaUser(),
                        KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
    }

    private List<Runnable> resources = new ArrayList<>();

    private <T extends HasMetadata> T deleteLater(MixedOperation<T, ?, ?, ?> x, T resource) {
        resources.add(0, () -> {
            LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
            x.delete(resource);
        });
        return resource;
    }

    Kafka deleteLater(Kafka resource) {
        return deleteLater(kafka(), resource);
    }

    KafkaTopic deleteLater(KafkaTopic resource) {
        return deleteLater(kafkaTopic(), resource);
    }

    KafkaUser deleteLater(KafkaUser resource) {
        return deleteLater(kafkaUser(), resource);
    }

    Job deleteLater(Job resource) {
        return deleteLater(client().extensions().jobs(), resource);
    }

    void deleteResources() {
        for (Runnable resource : resources) {
            resource.run();
        }
    }

    DoneableKafka kafkaEphemeral(String name, int kafkaReplicas) {
        return kafka(defaultKafka(name, kafkaReplicas).build());
    }

    public KafkaBuilder defaultKafka(String name, int kafkaReplicas) {
        return new KafkaBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
                    .withNewSpec()
                        .withNewKafka()
                            .withReplicas(kafkaReplicas)
                            .withNewEphemeralStorageStorage().endEphemeralStorageStorage()
                            .addToConfig("offsets.topic.replication.factor", Math.min(kafkaReplicas, 3))
                            .addToConfig("transaction.state.log.min.isr", Math.min(kafkaReplicas, 2))
                            .addToConfig("transaction.state.log.replication.factor", Math.min(kafkaReplicas, 3))
                            .withNewListeners()
                                .withNewPlain().endPlain()
                                .withNewTls().endTls()
                            .endListeners()
                            .withNewReadinessProbe()
                                .withInitialDelaySeconds(15)
                                .withTimeoutSeconds(5)
                            .endReadinessProbe()
                            .withNewLivenessProbe()
                                .withInitialDelaySeconds(15)
                                .withTimeoutSeconds(5)
                            .endLivenessProbe()
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(1)
                .withNewReadinessProbe()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .endReadinessProbe()
                .withNewLivenessProbe()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .endLivenessProbe()
                            .withNewEphemeralStorageStorage().endEphemeralStorageStorage()
                        .endZookeeper()
                        .withNewEntityOperator()
                            .withNewTopicOperator().withImage(TestUtils.changeOrgAndTag("strimzi/topic-operator:latest")).endTopicOperator()
                            .withNewUserOperator().withImage(TestUtils.changeOrgAndTag("strimzi/user-operator:latest")).endUserOperator()
                        .endEntityOperator()
                    .endSpec();
    }

    DoneableKafka kafka(Kafka kafka) {
        return new DoneableKafka(kafka, k -> {
            LOGGER.info("Creating Kafka\n{}", TestUtils.toYamlString(k));
            return waitFor(deleteLater(kafka().create(k)));
        });
    }

    Kafka waitFor(Kafka kafka) {
        String namespace = kafka.getMetadata().getNamespace();
        waitForStatefulSet(namespace, kafka.getMetadata().getName() + "-zookeeper");
        waitForStatefulSet(namespace, kafka.getMetadata().getName() + "-kafka");
        waitForDeployment(namespace, kafka.getMetadata().getName() + "-entity-operator");
        return kafka;
    }

    private void waitForStatefulSet(String namespace, String name) {
        LOGGER.info("Waiting for StatefulSet {}", name);
        TestUtils.waitFor("statefulset " + name, 1000, 300000,
            () -> client().apps().statefulSets().inNamespace(namespace).withName(name).isReady());
        int replicas = client().apps().statefulSets().inNamespace(namespace).withName(name).get().getSpec().getReplicas();
        for (int pod = 0; pod < replicas; pod++) {
            String podName = name + "-" + pod;
            LOGGER.info("Waiting for Pod {}", podName);
            TestUtils.waitFor("pod " + name, 1000, 300000,
                () -> client().pods().inNamespace(namespace).withName(podName).isReady());
            LOGGER.info("Pod {} is ready", podName);
        }
        LOGGER.info("StatefulSet {} is ready", name);
    }

    private void waitForDeployment(String namespace, String name) {
        LOGGER.info("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name, 1000, 300000,
            () -> client().extensions().deployments().inNamespace(namespace).withName(name).isReady());
        LOGGER.info("Deployment {} is ready", name);
    }

    DoneableKafkaTopic topic(String clusterName, String topicName) {
        return topic(defaultTopic(clusterName, topicName).build());
    }

    private KafkaTopicBuilder defaultTopic(String clusterName, String topicName) {
        return new KafkaTopicBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withName(topicName)
                                .withNamespace(client().getNamespace())
                                .addToLabels("strimzi.io/cluster", clusterName)
                .build())
                .withNewSpec()
                    .withPartitions(1)
                    .withReplicas(1)
                .endSpec();
    }

    DoneableKafkaTopic topic(KafkaTopic topic) {
        return new DoneableKafkaTopic(topic, kt -> {
            KafkaTopic resource = kafkaTopic().create(kt);
            LOGGER.info("Created KafkaTopic {}", resource.getMetadata().getName());
            return deleteLater(resource);
        });
    }

    DoneableKafkaUser tlsUser(String name) {
        return user(new KafkaUserBuilder().withMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(client().getNamespace())
                        .build())
                .withNewSpec()
                    .withNewKafkaUserTlsClientAuthenticationAuthentication()
                    .endKafkaUserTlsClientAuthenticationAuthentication()
                .endSpec()
                .build());
    }

    DoneableKafkaUser scramShaUser(String name) {
        return user(new KafkaUserBuilder().withMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(client().getNamespace())
                        .build())
                .withNewSpec()
                    .withNewKafkaUserScramSha512ClientAuthenticationAuthentication()
                    .endKafkaUserScramSha512ClientAuthenticationAuthentication()
                .endSpec()
                .build());
    }

    private DoneableKafkaUser user(KafkaUser user) {
        return new DoneableKafkaUser(user, ku -> {
            KafkaUser resource = kafkaUser().create(ku);
            LOGGER.info("Created KafkaUser {}", resource.getMetadata().getName());
            return deleteLater(resource);
        });
    }
}
