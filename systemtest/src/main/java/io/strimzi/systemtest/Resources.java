/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingList;
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
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

public class Resources {

    private static final Logger LOGGER = LogManager.getLogger(Resources.class);
    private static final long POLL_INTERVAL_FOR_RESOURCE_CREATION = Duration.ofSeconds(3).toMillis();
    public static final long POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(1).toMillis();
    /* Timeout for deployment config is bigger than the timeout for default resource readiness because of creating a new image
    during the deployment process.*/
    private static final long TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS = Duration.ofMinutes(7).toMillis();
    private static final long TIMEOUT_FOR_RESOURCE_CREATION = Duration.ofMinutes(5).toMillis();
    public static final long TIMEOUT_FOR_RESOURCE_READINESS = Duration.ofMinutes(7).toMillis();

    public static final String STRIMZI_PATH_TO_CO_CONFIG = "../install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml";
    public static final String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";

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
        return new CustomResourceOperationsImpl<T, L, D>(((DefaultKubernetesClient) client()).getHttpClient(), client().getConfiguration(), Crds.kafka().getSpec().getGroup(), Crds.kafka().getSpec().getVersion(), "kafkas", true, client().getNamespace(), null, true, null, null, false, resourceType, listClass, doneClass);
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

    private MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> kafkaMirrorMaker() {
        return client()
                .customResources(Crds.mirrorMaker(),
                        KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
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

    private MixedOperation<Deployment, DeploymentList, DoneableDeployment, Resource<Deployment, DoneableDeployment>> clusterOperator() {
        return customResourcesWithCascading(Deployment.class, DeploymentList.class, DoneableDeployment.class);
    }

    private MixedOperation<KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding, Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> kubernetesClusterRoleBinding() {
        return customResourcesWithCascading(KubernetesClusterRoleBinding.class, KubernetesClusterRoleBindingList.class, DoneableKubernetesClusterRoleBinding.class);
    }

    private MixedOperation<KubernetesRoleBinding, KubernetesRoleBindingList, DoneableKubernetesRoleBinding, Resource<KubernetesRoleBinding, DoneableKubernetesRoleBinding>> kubernetesRoleBinding() {
        return customResourcesWithCascading(KubernetesRoleBinding.class, KubernetesRoleBindingList.class, DoneableKubernetesRoleBinding.class);
    }

    private List<Runnable> resources = new ArrayList<>();

    private <T extends HasMetadata> T deleteLater(MixedOperation<T, ?, ?, ?> x, T resource) {
        LOGGER.info("Scheduled deletion of {} {}", resource.getKind(), resource.getMetadata().getName());
        switch (resource.getKind()) {
            case Kafka.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.delete(resource);
                    waitForDeletion((Kafka) resource);
                });
                break;
            case KafkaConnect.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.delete(resource);
                    waitForDeletion((KafkaConnect) resource);
                });
                break;
            case KafkaConnectS2I.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.delete(resource);
                    waitForDeletion((KafkaConnectS2I) resource);
                });
                break;
            case KafkaMirrorMaker.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.delete(resource);
                    waitForDeletion((KafkaMirrorMaker) resource);
                });
                break;
            case "ClusterRoleBinding":
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.delete(resource);
                    client.rbac().kubernetesClusterRoleBindings().delete((KubernetesClusterRoleBinding) resource);
                });
            default :
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
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

    private KafkaMirrorMaker deleteLater(KafkaMirrorMaker resource) {
        return deleteLater(kafkaMirrorMaker(), resource);
    }

    private KafkaTopic deleteLater(KafkaTopic resource) {
        return deleteLater(kafkaTopic(), resource);
    }

    private KafkaUser deleteLater(KafkaUser resource) {
        return deleteLater(kafkaUser(), resource);
    }

    private Deployment deleteLater(Deployment resource) {
        return deleteLater(clusterOperator(), resource);
    }

    private KubernetesClusterRoleBinding deleteLater(KubernetesClusterRoleBinding resource) {
        return deleteLater(kubernetesClusterRoleBinding(), resource);
    }

    private KubernetesRoleBinding deleteLater(KubernetesRoleBinding resource) {
        return deleteLater(kubernetesRoleBinding(), resource);
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
                            .withNewEphemeralStorage().endEphemeralStorage()
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
                            .withNewResources()
                                .withNewRequests()
                                    .withMemory("1G")
                                .endRequests()
                            .endResources()
                            .withMetrics(new HashMap<>())
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewResources()
                                .withNewRequests()
                                    .withMemory("1G")
                                .endRequests()
                            .endResources()
                            .withMetrics(new HashMap<>())
                            .withNewReadinessProbe()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .endReadinessProbe()
                .withNewLivenessProbe()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .endLivenessProbe()
                            .withNewEphemeralStorage().endEphemeralStorage()
                        .endZookeeper()
                        .withNewEntityOperator()
                            .withNewTopicOperator().withImage(TestUtils.changeOrgAndTag("strimzi/topic-operator:latest")).endTopicOperator()
                            .withNewUserOperator().withImage(TestUtils.changeOrgAndTag("strimzi/user-operator:latest")).endUserOperator()
                        .endEntityOperator()
                    .endSpec();
    }

    DoneableKafka kafka(Kafka kafka) {
        return new DoneableKafka(kafka, k -> {
            TestUtils.waitFor("Kafka creation", POLL_INTERVAL_FOR_RESOURCE_CREATION, TIMEOUT_FOR_RESOURCE_READINESS,
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
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(name))
                .withReplicas(kafkaConnectReplicas)
                .withNewResources()
                    .withNewRequests()
                        .withMemory("1G")
                    .endRequests()
                .endResources()
                .withMetrics(new HashMap<>())
            .endSpec();
    }

    private DoneableKafkaConnect kafkaConnect(KafkaConnect kafkaConnect) {
        return new DoneableKafkaConnect(kafkaConnect, kC -> {
            TestUtils.waitFor("KafkaConnect creation", POLL_INTERVAL_FOR_RESOURCE_CREATION, TIMEOUT_FOR_RESOURCE_CREATION,
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
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(name))
                .withReplicas(kafkaConnectS2IReplicas)
            .endSpec();
    }

    private DoneableKafkaConnectS2I kafkaConnectS2I(KafkaConnectS2I kafkaConnectS2I) {
        return new DoneableKafkaConnectS2I(kafkaConnectS2I, kCS2I -> {
            TestUtils.waitFor("KafkaConnectS2I creation", POLL_INTERVAL_FOR_RESOURCE_CREATION, TIMEOUT_FOR_RESOURCE_READINESS,
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

    DoneableKafkaMirrorMaker kafkaMirrorMaker(String name, String sourceBootstrapServer, String targetBootstrapServer, String groupId, int mirrorMakerReplicas, boolean tlsListener) {
        return kafkaMirrorMaker(defaultMirrorMaker(name, sourceBootstrapServer, targetBootstrapServer, groupId, mirrorMakerReplicas, tlsListener).build());
    }

    private KafkaMirrorMakerBuilder defaultMirrorMaker(String name, String sourceBootstrapServer, String targetBootstrapServer, String groupId, int mirrorMakerReplicas, boolean tlsListener) {
        return new KafkaMirrorMakerBuilder()
            .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
            .withNewSpec()
                .withNewConsumer()
                    .withBootstrapServers(tlsListener ? sourceBootstrapServer + "-kafka-bootstrap:9093" : sourceBootstrapServer + "-kafka-bootstrap:9092")
                    .withGroupId(groupId)
                .endConsumer()
                .withNewProducer()
                    .withBootstrapServers(tlsListener ? targetBootstrapServer + "-kafka-bootstrap:9093" : targetBootstrapServer + "-kafka-bootstrap:9092")
                .endProducer()
                .withNewResources()
                    .withNewRequests()
                        .withMemory("1G")
                    .endRequests()
                .endResources()
                .withMetrics(new HashMap<>())
            .withReplicas(mirrorMakerReplicas)
            .withWhitelist(".*")
            .endSpec();
    }

    private DoneableKafkaMirrorMaker kafkaMirrorMaker(KafkaMirrorMaker kafkaMirrorMaker) {
        return new DoneableKafkaMirrorMaker(kafkaMirrorMaker, k -> {
            TestUtils.waitFor("Kafka Mirror Maker creation", POLL_INTERVAL_FOR_RESOURCE_CREATION, TIMEOUT_FOR_RESOURCE_CREATION,
                () -> {
                    try {
                        kafkaMirrorMaker().inNamespace(client().getNamespace()).createOrReplace(k);
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
            return waitFor(deleteLater(k));
        });
    }

    /**
     * Wait until the ZK, Kafka and EO are all ready
     */
    private Kafka waitFor(Kafka kafka) {
        String name = kafka.getMetadata().getName();
        LOGGER.info("Waiting for Kafka {}", name);
        String namespace = kafka.getMetadata().getNamespace();
        waitForStatefulSet(namespace, KafkaResources.zookeeperStatefulSetName(name));
        waitForStatefulSet(namespace, KafkaResources.kafkaStatefulSetName(name));
        waitForDeployment(namespace, KafkaResources.entityOperatorDeploymentName(name));
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

    private KafkaMirrorMaker waitFor(KafkaMirrorMaker kafkaMirrorMaker) {
        LOGGER.info("Waiting for Kafka Mirror Maker {}", kafkaMirrorMaker.getMetadata().getName());
        String namespace = kafkaMirrorMaker.getMetadata().getNamespace();
        waitForDeployment(namespace, kafkaMirrorMaker.getMetadata().getName() + "-mirror-maker");
        return kafkaMirrorMaker;
    }

    private Deployment waitFor(Deployment clusterOperator) {
        LOGGER.info("Waiting for Cluster Operator {}", clusterOperator.getMetadata().getName());
        String namespace = client.getNamespace();
        waitForDeployment(namespace, clusterOperator.getMetadata().getName());
        return clusterOperator;
    }

    /**
     * Wait until the SS is ready and all of its Pods are also ready
     */
    public void waitForStatefulSet(String namespace, String name) {
        StUtils.waitForAllStatefulSetPodsReady(client(), namespace, name);
    }

    /**
     * Wait until the deployment is ready
     */
    private void waitForDeployment(String namespace, String name) {
        LOGGER.info("Waiting for Deployment {} in namespace {}", name, namespace);
        StUtils.waitForDeploymentReady(client(), namespace, name);
        LOGGER.info("Deployment {} is ready", name);
    }

    private void waitForDeploymentConfig(String namespace, String name) {
        LOGGER.info("Waiting for Deployment Config {}", name);
        TestUtils.waitFor("deployment config " + name, POLL_INTERVAL_FOR_RESOURCE_READINESS, TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS,
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

    private void waitForDeletion(KafkaMirrorMaker kafkaMirrorMaker) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Mirror Maker {}", kafkaMirrorMaker.getMetadata().getName());
        String namespace = kafkaMirrorMaker.getMetadata().getNamespace();

        client.pods().inNamespace(namespace).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(kafkaMirrorMaker.getMetadata().getName() + "-mirror-maker-"))
                .forEach(p -> waitForPodDeletion(namespace, p.getMetadata().getName()));
    }

    private void waitForPodDeletion(String namespace, String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("statefulset " + name, POLL_INTERVAL_FOR_RESOURCE_READINESS, TIMEOUT_FOR_RESOURCE_READINESS,
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

    DoneableKafkaUser tlsUser(String clusterName, String name) {
        return user(new KafkaUserBuilder().withMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(client().getNamespace())
                        .addToLabels("strimzi.io/cluster", clusterName)
                        .build())
                .withNewSpec()
                    .withNewKafkaUserTlsClientAuthentication()
                    .endKafkaUserTlsClientAuthentication()
                .endSpec()
                .build());
    }

    DoneableKafkaUser scramShaUser(String clusterName, String name) {
        return user(new KafkaUserBuilder().withMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(client().getNamespace())
                        .addToLabels("strimzi.io/cluster", clusterName)
                        .build())
                .withNewSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                    .endKafkaUserScramSha512ClientAuthentication()
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

    Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }

    DoneableDeployment clusterOperator(String namespace) {
        return clusterOperator(defaultCLusterOperator(namespace).build());
    }

    DeploymentBuilder defaultCLusterOperator(String namespace) {

        Deployment clusterOperator = getDeploymentFromYaml(STRIMZI_PATH_TO_CO_CONFIG);

        // Get env from config file
        List<EnvVar> envVars = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        // Get default CO image
        String coImage = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        // Update images
        for (EnvVar envVar : envVars) {
            switch (envVar.getName()) {
                case "STRIMZI_LOG_LEVEL":
                    envVar.setValue("DEBUG");
                    break;
                case "STRIMZI_NAMESPACE":
                    envVar.setValue(namespace);
                    envVar.setValueFrom(null);
                    break;
                case "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS":
                    envVar.setValue("30000");
                default:
                    if (envVar.getName().contains("STRIMZI_DEFAULT")) {
                        envVar.setValue(TestUtils.changeOrgAndTag(envVar.getValue()));
                    } else if (envVar.getName().contains("IMAGES")) {
                        envVar.setValue(TestUtils.changeOrgAndTagInImageMap(envVar.getValue()));
                    }
            }
        }
        // Apply updated env variables
        clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars);

        return new DeploymentBuilder(clusterOperator)
                .withApiVersion("apps/v1")
                .editSpec()
                    .withNewSelector()
                        .addToMatchLabels("name", STRIMZI_DEPLOYMENT_NAME)
                    .endSelector()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .withImage(TestUtils.changeOrgAndTag(coImage))
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec();
    }

    DoneableDeployment clusterOperator(Deployment clusterOperator) {
        return new DoneableDeployment(clusterOperator, co -> {
            TestUtils.waitFor("Cluster operator creation", POLL_INTERVAL_FOR_RESOURCE_CREATION, TIMEOUT_FOR_RESOURCE_CREATION,
                () -> {
                    try {
                        client.apps().deployments().createOrReplace(co);
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
                    co));
        });
    }

    KubernetesRoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KubernetesRoleBinding.class);
    }

    KubernetesClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KubernetesClusterRoleBinding.class);
    }

    DoneableKubernetesRoleBinding kubernetesRoleBinding(String yamlPath, String namespace, String clientNamespace) {
        return kubernetesRoleBinding(defaultKubernetesRoleBinding(yamlPath, namespace).build(), clientNamespace);
    }

    KubernetesRoleBindingBuilder defaultKubernetesRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating RoleBinding from {} in namespace {}", yamlPath, namespace);

        return new KubernetesRoleBindingBuilder(getRoleBindingFromYaml(yamlPath))
                .withApiVersion("rbac.authorization.k8s.io/v1")
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject();
    }

    DoneableKubernetesRoleBinding kubernetesRoleBinding(KubernetesRoleBinding roleBinding, String clientNamespace) {
        LOGGER.info("Apply RoleBinding in namespace {}", clientNamespace);
        client.inNamespace(clientNamespace).rbac().kubernetesRoleBindings().createOrReplace(roleBinding);
        deleteLater(roleBinding);
        return new DoneableKubernetesRoleBinding(roleBinding);
    }

    DoneableKubernetesClusterRoleBinding kubernetesClusterRoleBinding(String yamlPath, String namespace, String clientNamespace) {
        return kubernetesClusterRoleBinding(defaultKubernetesClusterRoleBinding(yamlPath, namespace).build(), clientNamespace);
    }

    KubernetesClusterRoleBindingBuilder defaultKubernetesClusterRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating ClusterRoleBinding from {} in namespace {}", yamlPath, namespace);

        return new KubernetesClusterRoleBindingBuilder(getClusterRoleBindingFromYaml(yamlPath))
                .withApiVersion("rbac.authorization.k8s.io/v1")
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject();
    }

    DoneableKubernetesClusterRoleBinding kubernetesClusterRoleBinding(KubernetesClusterRoleBinding clusterRoleBinding, String clientNamespace) {
        LOGGER.info("Apply ClusterRoleBinding in namespace {}", clientNamespace);
        client.inNamespace(clientNamespace).rbac().kubernetesClusterRoleBindings().createOrReplace(clusterRoleBinding);
        deleteLater(clusterRoleBinding);
        return new DoneableKubernetesClusterRoleBinding(clusterRoleBinding);
    }
}
