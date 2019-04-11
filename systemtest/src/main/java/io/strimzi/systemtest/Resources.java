/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.client.OpenShiftClient;
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
import java.util.Optional;
import java.util.stream.IntStream;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class Resources extends AbstractResources {

    private static final Logger LOGGER = LogManager.getLogger(Resources.class);
    private static final long POLL_INTERVAL_FOR_RESOURCE_CREATION = Duration.ofSeconds(3).toMillis();
    public static final long POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(1).toMillis();
    /* Timeout for deployment config is bigger than the timeout for default resource readiness because of creating a new image
    during the deployment process.*/
    private static final long TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS = Duration.ofMinutes(7).toMillis();
    private static final long TIMEOUT_FOR_RESOURCE_CREATION = Duration.ofMinutes(5).toMillis();
    public static final long TIMEOUT_FOR_RESOURCE_READINESS = Duration.ofMinutes(7).toMillis();
    private static final String KAFKA_VERSION = System.getenv().getOrDefault("ST_KAFKA_VERSION", "2.2.0");

    public static final String STRIMZI_PATH_TO_CO_CONFIG = "../install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml";
    public static final String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    public static final String STRIMZI_DEFAULT_LOG_LEVEL = "DEBUG";

    Resources(NamespacedKubernetesClient client) {
        super(client);
    }

    private List<Runnable> resources = new ArrayList<>();

    private <T extends HasMetadata> T deleteLater(MixedOperation<T, ?, ?, ?> x, T resource) {
        LOGGER.info("Scheduled deletion of {} {}", resource.getKind(), resource.getMetadata().getName());
        switch (resource.getKind()) {
            case Kafka.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.inNamespace(resource.getMetadata().getNamespace()).delete(resource);
                    waitForDeletion((Kafka) resource);
                });
                break;
            case KafkaConnect.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.inNamespace(resource.getMetadata().getNamespace()).delete(resource);
                    waitForDeletion((KafkaConnect) resource);
                });
                break;
            case KafkaConnectS2I.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.inNamespace(resource.getMetadata().getNamespace()).delete(resource);
                    waitForDeletion((KafkaConnectS2I) resource);
                });
                break;
            case KafkaMirrorMaker.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.inNamespace(resource.getMetadata().getNamespace()).delete(resource);
                    waitForDeletion((KafkaMirrorMaker) resource);
                });
                break;
            case "ClusterRoleBinding":
                resources.add(() -> {
                    LOGGER.info("Deleting {} {}", resource.getKind(), resource.getMetadata().getName());
                    x.delete(resource);
                    client.rbac().kubernetesClusterRoleBindings().delete((KubernetesClusterRoleBinding) resource);
                });
                break;
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
        String tOImage = TestUtils.changeOrgAndTag(getImageValueFromCO("STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE"));
        String uOImage = TestUtils.changeOrgAndTag(getImageValueFromCO("STRIMZI_DEFAULT_USER_OPERATOR_IMAGE"));

        return new KafkaBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
                    .withNewSpec()
                        .withNewKafka()
                            .withVersion(KAFKA_VERSION)
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
                            .withResources(new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("1G")).build())
                            .withMetrics(new HashMap<>())
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(3)
                .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("1G")).build())
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
                            .withNewTopicOperator().withImage(tOImage).endTopicOperator()
                            .withNewUserOperator().withImage(uOImage).endUserOperator()
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
                .withVersion(KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(name))
                .withReplicas(kafkaConnectReplicas)
                .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("1G")).build())
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
                .withVersion(KAFKA_VERSION)
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
                .withVersion(KAFKA_VERSION)
                .withNewConsumer()
                    .withBootstrapServers(tlsListener ? sourceBootstrapServer + "-kafka-bootstrap:9093" : sourceBootstrapServer + "-kafka-bootstrap:9092")
                    .withGroupId(groupId)
                .endConsumer()
                .withNewProducer()
                    .withBootstrapServers(tlsListener ? targetBootstrapServer + "-kafka-bootstrap:9093" : targetBootstrapServer + "-kafka-bootstrap:9092")
                .endProducer()
                .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("1G")).build())
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

    private KafkaConnect waitFor(KafkaConnect kafkaConnect) {
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
    private void waitForStatefulSet(String namespace, String name) {
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

    private Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }

    DoneableDeployment clusterOperator(String namespace) {
        return clusterOperator(namespace, "300000");
    }

    DoneableDeployment clusterOperator(String namespace, String operationTimeout) {
        return clusterOperator(defaultCLusterOperator(namespace, operationTimeout).build());
    }

    DeploymentBuilder defaultCLusterOperator(String namespace, String operationTimeout) {

        Deployment clusterOperator = getDeploymentFromYaml(STRIMZI_PATH_TO_CO_CONFIG);

        // Get env from config file
        List<EnvVar> envVars = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        // Get default CO image
        String coImage = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        // Update images
        for (EnvVar envVar : envVars) {
            switch (envVar.getName()) {
                case "STRIMZI_LOG_LEVEL":
                    envVar.setValue(System.getenv().getOrDefault("TEST_STRIMZI_LOG_LEVEL", STRIMZI_DEFAULT_LOG_LEVEL));
                    break;
                case "STRIMZI_NAMESPACE":
                    envVar.setValue(namespace);
                    envVar.setValueFrom(null);
                    break;
                case "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS":
                    envVar.setValue("30000");
                    break;
                case "STRIMZI_OPERATION_TIMEOUT_MS":
                    envVar.setValue(operationTimeout);
                    break;
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

    private KubernetesRoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KubernetesRoleBinding.class);
    }

    private KubernetesClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KubernetesClusterRoleBinding.class);
    }

    DoneableKubernetesRoleBinding kubernetesRoleBinding(String yamlPath, String namespace, String clientNamespace) {
        return kubernetesRoleBinding(defaultKubernetesRoleBinding(yamlPath, namespace).build(), clientNamespace);
    }

    private KubernetesRoleBindingBuilder defaultKubernetesRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating RoleBinding from {} in namespace {}", yamlPath, namespace);

        return new KubernetesRoleBindingBuilder(getRoleBindingFromYaml(yamlPath))
                .withApiVersion("rbac.authorization.k8s.io/v1")
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject();
    }

    private DoneableKubernetesRoleBinding kubernetesRoleBinding(KubernetesRoleBinding roleBinding, String clientNamespace) {
        LOGGER.info("Apply RoleBinding in namespace {}", clientNamespace);
        client.inNamespace(clientNamespace).rbac().kubernetesRoleBindings().createOrReplace(roleBinding);
        deleteLater(roleBinding);
        return new DoneableKubernetesRoleBinding(roleBinding);
    }

    DoneableKubernetesClusterRoleBinding kubernetesClusterRoleBinding(String yamlPath, String namespace, String clientNamespace) {
        return kubernetesClusterRoleBinding(defaultKubernetesClusterRoleBinding(yamlPath, namespace).build(), clientNamespace);
    }

    private KubernetesClusterRoleBindingBuilder defaultKubernetesClusterRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating ClusterRoleBinding from {} in namespace {}", yamlPath, namespace);

        return new KubernetesClusterRoleBindingBuilder(getClusterRoleBindingFromYaml(yamlPath))
                .withApiVersion("rbac.authorization.k8s.io/v1")
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject();
    }

    List<KubernetesClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespace) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");

        List<KubernetesClusterRoleBinding> kCRBList = new ArrayList<>();

        kCRBList.add(
            new KubernetesClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-cluster-operator-namespaced")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-cluster-operator-namespaced")
                .endRoleRef()
                .withSubjects(new KubernetesSubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new KubernetesClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-entity-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-entity-operator")
                .endRoleRef()
                .withSubjects(new KubernetesSubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new KubernetesClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-topic-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-topic-operator")
                .endRoleRef()
                .withSubjects(new KubernetesSubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );
        return kCRBList;
    }

    DoneableKubernetesClusterRoleBinding kubernetesClusterRoleBinding(KubernetesClusterRoleBinding clusterRoleBinding, String clientNamespace) {
        LOGGER.info("Apply ClusterRoleBinding in namespace {}", clientNamespace);
        client.inNamespace(clientNamespace).rbac().kubernetesClusterRoleBindings().createOrReplace(clusterRoleBinding);
        deleteLater(clusterRoleBinding);
        return new DoneableKubernetesClusterRoleBinding(clusterRoleBinding);
    }

    private String getImageValueFromCO(String name) {
        Deployment clusterOperator = getDeploymentFromYaml(STRIMZI_PATH_TO_CO_CONFIG);

        List<EnvVar> listEnvVar = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        Optional<EnvVar> envVar = listEnvVar.stream().filter(e -> e.getName().equals(name)).findFirst();
        if (envVar.isPresent()) {
            return envVar.get().getValue();
        }
        return "";
    }
}
