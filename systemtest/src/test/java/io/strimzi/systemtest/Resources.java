/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.internal.CustomResourceOperationsImpl;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.KafkaConnectS2IAssemblyList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

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
        return customResourcesWithCascading(Kafka.class, KafkaAssemblyList.class, DoneableKafka.class);
    }

    // This logic is necessary only for the deletion of resources with `cascading: true`
    private <T extends HasMetadata, L extends KubernetesResourceList, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>> customResourcesWithCascading(Class<T> resourceType, Class<L> listClass, Class<D> doneClass) {
        return new CustomResourceOperationsImpl<T, L, D>(((DefaultKubernetesClient) client()).getHttpClient(), client().getConfiguration(), Crds.kafka().getSpec().getGroup(), Crds.kafka().getSpec().getVersion(), "kafkas",  client().getNamespace(), null, true, null, null, false, resourceType, listClass, doneClass);
    }

    private MixedOperation<KafkaConnect, KafkaConnectAssemblyList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnect() {
        return client()
                .customResources(Crds.kafkaConnect(),
                        KafkaConnect.class, KafkaConnectAssemblyList.class, DoneableKafkaConnect.class);
    }

    private MixedOperation<KafkaConnectS2I, KafkaConnectS2IAssemblyList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2I() {
        return client()
                .customResources(Crds.kafkaConnectS2I(),
                        KafkaConnectS2I.class, KafkaConnectS2IAssemblyList.class, DoneableKafkaConnectS2I.class);
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
        LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
        switch (resource.getKind()) {
            case Kafka.RESOURCE_KIND:
                resources.add(() -> {
                    x.delete(resource);
                    waitForDeletion((Kafka) resource);
                });
                break;
            case KafkaConnect.RESOURCE_KIND:
                resources.add(() -> {
                    x.delete(resource);
                    waitForDeletion((KafkaConnect) resource);
                });
                break;
            case KafkaConnectS2I.RESOURCE_KIND:
                resources.add(() -> {
                    x.delete(resource);
                    waitForDeletion((KafkaConnectS2I) resource);
                });
                break;
            default :
                resources.add(() -> {
                    x.delete(resource);
                });
        }
        return resource;
    }

    private Kafka deleteLater(Kafka resource) {
        return deleteLater(kafka(), resource);
    }

    private KafkaConnect deleteLater(KafkaConnect resource) {
        return deleteLater(kafkaConnect(), resource);
    }

    private KafkaConnectS2I deleteLater(KafkaConnectS2I resource) {
        return deleteLater(kafkaConnectS2I(), resource);
    }

    private KafkaTopic deleteLater(KafkaTopic resource) {
        return deleteLater(kafkaTopic(), resource);
    }

    private KafkaUser deleteLater(KafkaUser resource) {
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
            TestUtils.waitFor("Kafka creation", 60000, 5000,
                () -> {
                    try {
                        kafka().inNamespace(client().getNamespace()).createOrReplace(k);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(
                    k));
        });
    }

    DoneableKafkaConnect kafkaConnect(String name, int kafkaConnectReplicas) {
        return kafkaConnect(defaultKafkaConnect(name, kafkaConnectReplicas).build());
    }

    private KafkaConnectBuilder defaultKafkaConnect(String name, int kafkaConnectReplicas) {
        return new KafkaConnectBuilder()
            .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
            .withNewSpec()
                .withBootstrapServers(name + "-kafka-bootstrap:9092")
                .withReplicas(kafkaConnectReplicas)
            .endSpec();
    }

    private DoneableKafkaConnect kafkaConnect(KafkaConnect kafkaConnect) {
        return new DoneableKafkaConnect(kafkaConnect, kC -> {
            TestUtils.waitFor("KafkaConnect creation", 60000, 5000,
                () -> {
                    try {
                        kafkaConnect().inNamespace(client().getNamespace()).createOrReplace(kC);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(
                    kC));
        });
    }

    /**
     * Method to create Kafka Connect S2I using OpenShift client. This method can only be used if you run system tests on the OpenShift platform because of adapting fabric8 client to ({@link OpenShiftClient}) on waiting stage.
     * @param name Kafka Connect S2I name
     * @param kafkaConnectS2IReplicas the number of replicas
     * @return Kafka Connect S2I
     */
    DoneableKafkaConnectS2I kafkaConnectS2I(String name, int kafkaConnectS2IReplicas) {
        return kafkaConnectS2I(defaultKafkaConnectS2I(name, kafkaConnectS2IReplicas).build());
    }

    private KafkaConnectS2IBuilder defaultKafkaConnectS2I(String name, int kafkaConnectS2IReplicas) {
        return new KafkaConnectS2IBuilder()
            .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
            .withNewSpec()
                .withBootstrapServers(name + "-kafka-bootstrap:9092")
                .withReplicas(kafkaConnectS2IReplicas)
            .endSpec();
    }

    private DoneableKafkaConnectS2I kafkaConnectS2I(KafkaConnectS2I kafkaConnectS2I) {
        return new DoneableKafkaConnectS2I(kafkaConnectS2I, kCS2I -> {
            TestUtils.waitFor("KafkaConnectS2I creation", 60000, 5000,
                () -> {
                    try {
                        kafkaConnectS2I().inNamespace(client().getNamespace()).createOrReplace(kCS2I);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(
                    kCS2I));
        });
    }

    private Kafka waitFor(Kafka kafka) {
        LOGGER.info("Waiting for Kafka {}", kafka.getMetadata().getName());
        String namespace = kafka.getMetadata().getNamespace();
        waitForStatefulSet(namespace, kafka.getMetadata().getName() + "-zookeeper");
        waitForStatefulSet(namespace, kafka.getMetadata().getName() + "-kafka");
        waitForDeployment(namespace, kafka.getMetadata().getName() + "-entity-operator");
        return kafka;
    }

    KafkaConnect waitFor(KafkaConnect kafkaConnect) {
        LOGGER.info("Waiting for Kafka Connect {}", kafkaConnect.getMetadata().getName());
        String namespace = kafkaConnect.getMetadata().getNamespace();
        waitForDeployment(namespace, kafkaConnect.getMetadata().getName() + "-connect");
        return kafkaConnect;
    }

    private KafkaConnectS2I waitFor(KafkaConnectS2I kafkaConnectS2I) {
        LOGGER.info("Waiting for Kafka Connect S2I {}", kafkaConnectS2I.getMetadata().getName());
        String namespace = kafkaConnectS2I.getMetadata().getNamespace();
        waitForDeploymentConfig(namespace, kafkaConnectS2I.getMetadata().getName() + "-connect");
        return kafkaConnectS2I;
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

    private void waitForDeploymentConfig(String namespace, String name) {
        LOGGER.info("Waiting for Deployment Config {}", name);
        TestUtils.waitFor("deployment config " + name, 1000, 300000,
            () -> client().adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(namespace).withName(name).isReady());
        LOGGER.info("Deployment Config {} is ready", name);
    }

    private void waitForDeletion(Kafka kafka) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka {}", kafka.getMetadata().getName());
        String namespace = kafka.getMetadata().getNamespace();

        IntStream.rangeClosed(0, kafka.getSpec().getZookeeper().getReplicas() - 1).forEach(podIndex ->
            waitForPodDeletion(namespace, kafka.getMetadata().getName() + "-zookeeper-" + podIndex));

        IntStream.rangeClosed(0, kafka.getSpec().getKafka().getReplicas() - 1).forEach(podIndex ->
            waitForPodDeletion(namespace, kafka.getMetadata().getName() + "-kafka-" + podIndex));
    }

    private void waitForDeletion(KafkaConnect kafkaConnect) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Connect {}", kafkaConnect.getMetadata().getName());
        String namespace = kafkaConnect.getMetadata().getNamespace();

        client.pods().inNamespace(namespace).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(kafkaConnect.getMetadata().getName() + "-connect-"))
                .forEach(p -> waitForPodDeletion(namespace, p.getMetadata().getName()));
    }

    private void waitForDeletion(KafkaConnectS2I kafkaConnectS2I) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Connect S2I {}", kafkaConnectS2I.getMetadata().getName());
        String namespace = kafkaConnectS2I.getMetadata().getNamespace();

        client.pods().inNamespace(namespace).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(kafkaConnectS2I.getMetadata().getName() + "-connect-"))
                .forEach(p -> waitForPodDeletion(namespace, p.getMetadata().getName()));
    }

    private void waitForPodDeletion(String namespace, String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);
        TestUtils.waitFor("statefulset " + name, 1000, 300000,
            () -> client().pods().inNamespace(namespace).withName(name).get() == null);
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

    DoneableKafkaUser user(KafkaUser user) {
        return new DoneableKafkaUser(user, ku -> {
            KafkaUser resource = kafkaUser().inNamespace(client().getNamespace()).createOrReplace(ku);
            LOGGER.info("Created KafkaUser {}", resource.getMetadata().getName());
            return deleteLater(resource);
        });
    }
}
