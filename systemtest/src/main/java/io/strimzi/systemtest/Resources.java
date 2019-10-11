/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableIngress;
import io.fabric8.kubernetes.api.model.extensions.HTTPIngressPath;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.extensions.IngressBackend;
import io.fabric8.kubernetes.api.model.extensions.IngressBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.IntStream;

import static io.strimzi.systemtest.AbstractST.CLUSTER_NAME;
import static io.strimzi.test.TestUtils.toYamlString;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class Resources extends AbstractResources {

    private static final Logger LOGGER = LogManager.getLogger(Resources.class);
    private static final String KAFKA_VERSION = Environment.ST_KAFKA_VERSION;

    public static final String STRIMZI_PATH_TO_CO_CONFIG = "../install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml";
    public static final String PATH_TO_KAFKA_METRICS_CONFIG = "../metrics/examples/kafka/kafka-metrics.yaml";
    public static final String PATH_TO_KAFKA_CONNECT_METRICS_CONFIG = "../metrics/examples/kafka/kafka-connect-metrics.yaml";

    public static final String PATH_TO_KAFKA_CONNECT_OAUTH_CONFIG = "../systemtest/src/test/resources/oauth2/kafka-connect-oauth.yaml";

    private static final String DEPLOYMENT = "Deployment";
    private static final String SERVICE = "Service";
    private static final String INGRESS = "Ingress";
    private static final String CLUSTER_ROLE_BINDING = "ClusterRoleBinding";
    private static final String ROLE_BINDING = "RoleBinding";

    private Stack<Runnable> resources = new Stack<>();

    Resources(KubeClient client) {
        super(client);
    }

    @SuppressWarnings("unchecked")
    private <T extends HasMetadata> T deleteLater(MixedOperation<T, ?, ?, ?> x, T resource) {
        LOGGER.info("Scheduled deletion of {} {} in namespace {}",
                resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
        switch (resource.getKind()) {
            case Kafka.RESOURCE_KIND:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
                    waitForDeletion((Kafka) resource);
                });
                break;
            case KafkaConnect.RESOURCE_KIND:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
                    waitForDeletion((KafkaConnect) resource);
                });
                break;
            case KafkaConnectS2I.RESOURCE_KIND:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
                    waitForDeletion((KafkaConnectS2I) resource);
                });
                break;
            case KafkaMirrorMaker.RESOURCE_KIND:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
                    waitForDeletion((KafkaMirrorMaker) resource);
                });
                break;
            case KafkaBridge.RESOURCE_KIND:
                resources.add(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
                    waitForDeletion((KafkaBridge) resource);
                });
                break;
            case DEPLOYMENT:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    waitForDeletion((Deployment) resource);
                });
                break;
            case CLUSTER_ROLE_BINDING:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    client().getClient().rbac().clusterRoleBindings().withName(resource.getMetadata().getName()).delete();
                });
                break;
            case ROLE_BINDING:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    client().getClient().rbac().roleBindings().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
                });
                break;
            case SERVICE:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    client().getClient().services().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
                });
                break;
            case INGRESS:
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
                    client().deleteIngress((Ingress) resource);
                });
                break;
            default :
                resources.push(() -> {
                    LOGGER.info("Deleting {} {} in namespace {}",
                            resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                    x.inNamespace(resource.getMetadata().getNamespace())
                            .withName(resource.getMetadata().getName())
                            .delete();
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

    private KafkaBridge deleteLater(KafkaBridge resource) {
        return deleteLater(kafkaBridge(), resource);
    }

    private KafkaTopic deleteLater(KafkaTopic resource) {
        return deleteLater(kafkaTopic(), resource);
    }

    private KafkaUser deleteLater(KafkaUser resource) {
        return deleteLater(kafkaUser(), resource);
    }

    private Deployment deleteLater(Deployment resource) {
        return deleteLater(deployment(), resource);
    }

    private ClusterRoleBinding deleteLater(ClusterRoleBinding resource) {
        return deleteLater(clusterRoleBinding(), resource);
    }

    private RoleBinding deleteLater(RoleBinding resource) {
        return deleteLater(roleBinding(), resource);
    }

    private Service deleteLater(Service resource) {
        return deleteLater(service(), resource);
    }

    private Ingress deleteLater(Ingress resource) {
        return deleteLater(ingress(), resource);
    }

    void deleteResources() {
        while (!resources.empty()) {
            resources.pop().run();
        }
    }

    public DoneableKafka kafkaEphemeral(String name, int kafkaReplicas) {
        return kafkaEphemeral(name, kafkaReplicas, 3);
    }

    public DoneableKafka kafkaEphemeral(String name, int kafkaReplicas, int zookeeperReplicas) {
        return kafka(defaultKafka(name, kafkaReplicas, zookeeperReplicas).build());
    }

    public DoneableKafka kafkaJBOD(String name, int kafkaReplicas, JbodStorage jbodStorage) {
        return kafka(defaultKafka(name, kafkaReplicas).
                editSpec()
                    .editKafka()
                        .withStorage(jbodStorage)
                    .endKafka()
                    .editZookeeper().
                        withReplicas(1)
                    .endZookeeper()
                .endSpec()
                .build());
    }

    public KafkaBuilder defaultKafka(String name, int kafkaReplicas) {
        return defaultKafka(name, kafkaReplicas, 3);
    }

    public KafkaBuilder defaultKafka(String name, int kafkaReplicas, int zookeeperReplicas) {
        String tOImage = StUtils.changeOrgAndTag(getImageValueFromCO("STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE"));
        String uOImage = StUtils.changeOrgAndTag(getImageValueFromCO("STRIMZI_DEFAULT_USER_OPERATOR_IMAGE"));

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
                            .withNewJvmOptions()
                                .withGcLoggingEnabled(false)
                            .endJvmOptions()
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(zookeeperReplicas)
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
                            .withNewJvmOptions()
                                .withGcLoggingEnabled(false)
                            .endJvmOptions()
                        .endZookeeper()
                        .withNewEntityOperator()
                            .withNewTopicOperator().withImage(tOImage).endTopicOperator()
                            .withNewUserOperator().withImage(uOImage).endUserOperator()
                        .endEntityOperator()
                    .endSpec();
    }

    public DoneableKafka kafkaWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        return kafka(defaultKafkaWithMetrics(name, kafkaReplicas, zookeeperReplicas).build());
    }

    private KafkaBuilder defaultKafkaWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(PATH_TO_KAFKA_METRICS_CONFIG);

        return new KafkaBuilder(kafka)
            .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
            .editSpec()
                .editKafka()
                    .withVersion(Environment.ST_KAFKA_VERSION)
                    .withReplicas(kafkaReplicas)
                .endKafka()
                .editZookeeper()
                    .withReplicas(zookeeperReplicas)
                .endZookeeper()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec();
    }

    DoneableKafka kafka(Kafka kafka) {
        return new DoneableKafka(kafka, k -> {
            TestUtils.waitFor("Kafka creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
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

    /**
     * This method is used for deploy specific Kafka cluster without wait for all resources.
     * It can be use for example for deploy Kafka cluster with unsupported Kafka version.
     * @param kafka kafka cluster specification
     * @return kafka cluster specification
     */
    public Kafka kafkaWithoutWait(Kafka kafka) {
        kafka().inNamespace(client().getNamespace()).createOrReplace(kafka);
        return kafka;
    }

    /**
     * This method is used for delete specific Kafka cluster without wait for all resources deletion.
     * It can be use for example for delete Kafka cluster CR with unsupported Kafka version.
     * @param kafka kafka cluster specification
     */
    public void deleteKafkaWithoutWait(Kafka kafka) {
        kafka().inNamespace(client().getNamespace()).delete(kafka);
    }

    public DoneableKafkaConnect kafkaConnect(String name, int kafkaConnectReplicas) {
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

    public DoneableKafkaConnect kafkaConnectWithMetrics(String name, int kafkaConnectReplicas) {
        return kafkaConnect(defaultKafkaConnectWithMetrics(name, kafkaConnectReplicas).build());
    }

    private KafkaConnectBuilder defaultKafkaConnectWithMetrics(String name, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(PATH_TO_KAFKA_CONNECT_METRICS_CONFIG);

        return new KafkaConnectBuilder(kafkaConnect)
                .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
                .editSpec()
                    .withVersion(KAFKA_VERSION)
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(name))
                    .withReplicas(kafkaConnectReplicas)
                    .withResources(new ResourceRequirementsBuilder()
                            .addToRequests("memory", new Quantity("1G")).build())
                .endSpec();
    }

    public DoneableKafkaConnect kafkaConnectWithOauth(String name, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(PATH_TO_KAFKA_CONNECT_OAUTH_CONFIG);

        return kafkaConnect(new KafkaConnectBuilder(kafkaConnect)
                .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
                .editSpec()
                    .withVersion(KAFKA_VERSION)
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(name))
                    .withReplicas(kafkaConnectReplicas)
                    .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("1G")).build())
                .endSpec()
                .build());
    }

    public DoneableKafkaConnect kafkaConnectWithTracing(String name) {
        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        return kafkaConnect(defaultKafkaConnect(name, 1)
                .editSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue("my-target-connect")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue("1")
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build());
    }

    private DoneableKafkaConnect kafkaConnect(KafkaConnect kafkaConnect) {
        return new DoneableKafkaConnect(kafkaConnect, kC -> {
            TestUtils.waitFor("KafkaConnect creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
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
     * @param kafkaConnectS2IReplicas the number of replicas
     * @return Kafka Connect S2I
     */
    DoneableKafkaConnectS2I kafkaConnectS2I(String name, String kafkaClusterName, int kafkaConnectS2IReplicas) {
        return kafkaConnectS2I(defaultKafkaConnectS2I(name, kafkaClusterName, kafkaConnectS2IReplicas).build());
    }

    private KafkaConnectS2IBuilder defaultKafkaConnectS2I(String name, String kafkaClusterName, int kafkaConnectS2IReplicas) {
        return new KafkaConnectS2IBuilder()
            .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
            .withNewSpec()
                .withVersion(KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withReplicas(kafkaConnectS2IReplicas)
                .withNewTls()
                .withTrustedCertificates(new CertSecretSourceBuilder().withNewSecretName(kafkaClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
                .endTls()
            .endSpec();
    }

    private DoneableKafkaConnectS2I kafkaConnectS2I(KafkaConnectS2I kafkaConnectS2I) {
        return new DoneableKafkaConnectS2I(kafkaConnectS2I, kCS2I -> {
            TestUtils.waitFor("KafkaConnectS2I creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
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

    public DoneableKafkaMirrorMaker kafkaMirrorMaker(String name, String sourceBootstrapServer, String targetBootstrapServer, String groupId, int mirrorMakerReplicas, boolean tlsListener) {
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
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewProducer()
                    .withBootstrapServers(tlsListener ? targetBootstrapServer + "-kafka-bootstrap:9093" : targetBootstrapServer + "-kafka-bootstrap:9092")
                    .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
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
            TestUtils.waitFor("Kafka Mirror Maker creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
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
        String namespace = kafka.getMetadata().getNamespace();
        LOGGER.info("Waiting for Kafka {} in namespace {}", name, namespace);
        LOGGER.info("Waiting for Zookeeper pods");
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(name), kafka.getSpec().getZookeeper().getReplicas());
        LOGGER.info("Zookeeper pods are ready");
        LOGGER.info("Waiting for Kafka pods");
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(name), kafka.getSpec().getKafka().getReplicas());
        LOGGER.info("Kafka pods are ready");
        // EO should not be deployed if it does not contain UO and TO
        if (kafka.getSpec().getEntityOperator().getTopicOperator() != null || kafka.getSpec().getEntityOperator().getUserOperator() != null) {
            LOGGER.info("Waiting for Entity Operator pods");
            StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(name));
            LOGGER.info("Entity Operator pods are ready");
        }
        // Kafka Exporter is not setup everytime
        if (kafka.getSpec().getKafkaExporter() != null) {
            LOGGER.info("Waiting for Kafka Exporter pods");
            StUtils.waitForDeploymentReady(KafkaExporterResources.deploymentName(name));
            LOGGER.info("Kafka Exporter pods are ready");
        }
        return kafka;
    }

    private KafkaConnect waitFor(KafkaConnect kafkaConnect) {
        LOGGER.info("Waiting for Kafka Connect {}", kafkaConnect.getMetadata().getName());
        StUtils.waitForDeploymentReady(kafkaConnect.getMetadata().getName() + "-connect", kafkaConnect.getSpec().getReplicas());
        LOGGER.info("Kafka Connect {} is ready", kafkaConnect.getMetadata().getName());
        return kafkaConnect;
    }

    private KafkaConnectS2I waitFor(KafkaConnectS2I kafkaConnectS2I) {
        LOGGER.info("Waiting for Kafka Connect S2I {}", kafkaConnectS2I.getMetadata().getName());
        StUtils.waitForDeploymentConfigReady(kafkaConnectS2I.getMetadata().getName() + "-connect", kafkaConnectS2I.getSpec().getReplicas());
        LOGGER.info("Kafka Connect S2I {} is ready", kafkaConnectS2I.getMetadata().getName());
        return kafkaConnectS2I;
    }

    private KafkaMirrorMaker waitFor(KafkaMirrorMaker kafkaMirrorMaker) {
        LOGGER.info("Waiting for Kafka Mirror Maker {}", kafkaMirrorMaker.getMetadata().getName());
        StUtils.waitForDeploymentReady(kafkaMirrorMaker.getMetadata().getName() + "-mirror-maker", kafkaMirrorMaker.getSpec().getReplicas());
        LOGGER.info("Kafka Mirror Maker {} is ready", kafkaMirrorMaker.getMetadata().getName());
        return kafkaMirrorMaker;
    }

    private KafkaBridge waitFor(KafkaBridge kafkaBridge) {
        LOGGER.info("Waiting for Kafka Bridge {}", kafkaBridge.getMetadata().getName());
        StUtils.waitForDeploymentReady(kafkaBridge.getMetadata().getName() + "-bridge", kafkaBridge.getSpec().getReplicas());
        LOGGER.info("Kafka Bridge {} is ready", kafkaBridge.getMetadata().getName());
        return kafkaBridge;
    }

    private Deployment waitFor(Deployment deployment) {
        LOGGER.info("Waiting for deployment {}", deployment.getMetadata().getName());
        StUtils.waitForDeploymentReady(deployment.getMetadata().getName(), deployment.getSpec().getReplicas());
        LOGGER.info("Deployment {} is ready", deployment.getMetadata().getName());
        return deployment;
    }

    private void waitForDeletion(Kafka kafka) {
        String kafkaClusterName = kafka.getMetadata().getName();
        LOGGER.info("Waiting when all the pods are terminated for Kafka {}", kafkaClusterName);

        StUtils.waitForStatefulSetDeletion(KafkaResources.zookeeperStatefulSetName(kafkaClusterName));

        IntStream.rangeClosed(0, kafka.getSpec().getZookeeper().getReplicas() - 1).forEach(podIndex ->
            StUtils.waitForPodDeletion(kafka.getMetadata().getName() + "-zookeeper-" + podIndex));

        StUtils.waitForStatefulSetDeletion(KafkaResources.kafkaStatefulSetName(kafkaClusterName));

        IntStream.rangeClosed(0, kafka.getSpec().getKafka().getReplicas() - 1).forEach(podIndex ->
                StUtils.waitForPodDeletion(kafka.getMetadata().getName() + "-kafka-" + podIndex));

        // Wait for EO deletion
        StUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(kafkaClusterName));
        StUtils.waitForReplicaSetDeletion(KafkaResources.entityOperatorDeploymentName(kafkaClusterName));

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().contains(kafka.getMetadata().getName() + "-entity-operator"))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));

        // Wait for Kafka Exporter deletion
        StUtils.waitForDeploymentDeletion(KafkaExporterResources.deploymentName(kafkaClusterName));
        StUtils.waitForReplicaSetDeletion(KafkaExporterResources.deploymentName(kafkaClusterName));

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().contains(kafka.getMetadata().getName() + "-kafka-exporter"))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));
    }

    private void waitForDeletion(KafkaConnect kafkaConnect) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Connect {}", kafkaConnect.getMetadata().getName());

        StUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnect.getMetadata().getName()));
        StUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnect.getMetadata().getName()));

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(kafkaConnect.getMetadata().getName() + "-connect-"))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));
    }

    private void waitForDeletion(KafkaConnectS2I kafkaConnectS2I) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Connect S2I {}", kafkaConnectS2I.getMetadata().getName());

        StUtils.waitForDeploymentConfigDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));
        StUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().contains("-connect-"))
                .forEach(p -> {
                    LOGGER.debug("Deleting: {}", p.getMetadata().getName());
                    client().deletePod(p);
                });
    }

    private void waitForDeletion(KafkaMirrorMaker kafkaMirrorMaker) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Mirror Maker {}", kafkaMirrorMaker.getMetadata().getName());

        StUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMaker.getMetadata().getName()));
        StUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMaker.getMetadata().getName()));

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(kafkaMirrorMaker.getMetadata().getName() + "-mirror-maker-"))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));
    }

    private void waitForDeletion(KafkaBridge kafkaBridge) {
        LOGGER.info("Waiting when all the pods are terminated for Kafka Bridge {}", kafkaBridge.getMetadata().getName());

        StUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(kafkaBridge.getMetadata().getName()));
        StUtils.waitForReplicaSetDeletion(KafkaMirrorMakerResources.deploymentName(kafkaBridge.getMetadata().getName()));

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(kafkaBridge.getMetadata().getName() + "-bridge-"))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));
    }

    private void waitForDeletion(Deployment deployment) {
        LOGGER.info("Waiting when all the pods are terminated for Deployment {}", deployment.getMetadata().getName());

        StUtils.waitForDeploymentDeletion(deployment.getMetadata().getName());

        client().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(deployment.getMetadata().getName()))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));
    }

    @Deprecated
    private void waitForPodDeletion(String name) {
        LOGGER.info("Waiting when Pod {} in namespace {} will be deleted", name, client().getNamespace());

        TestUtils.waitFor("pod " + name + " deletion", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> client().getPod(name) == null);
    }

    public DoneableKafkaTopic topic(String clusterName, String topicName) {
        return topic(defaultTopic(clusterName, topicName, 1, 1).build());
    }

    public DoneableKafkaTopic topic(String clusterName, String topicName, int partitions) {
        return topic(defaultTopic(clusterName, topicName, partitions, 1).build());
    }

    public DoneableKafkaTopic topic(String clusterName, String topicName, int partitions, int replicas) {
        return topic(defaultTopic(clusterName, topicName, partitions, replicas)
            .editSpec()
            .addToConfig("min.insync.replicas", replicas)
            .endSpec().build());
    }

    private KafkaTopicBuilder defaultTopic(String clusterName, String topicName, int partitions, int replicas) {
        LOGGER.info("Creating topic: {} with {} partitions and {} replicas", topicName, partitions, replicas);
        return new KafkaTopicBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withName(topicName)
                                .withNamespace(client().getNamespace())
                                .addToLabels("strimzi.io/cluster", clusterName)
                .build())
                .withNewSpec()
                    .withPartitions(partitions)
                    .withReplicas(replicas)
                .endSpec();
    }

    DoneableKafkaTopic topic(KafkaTopic topic) {
        return new DoneableKafkaTopic(topic, kt -> {
            KafkaTopic resource = kafkaTopic().inNamespace(topic.getMetadata().getNamespace()).create(kt);
            LOGGER.info("Created KafkaTopic {}", resource.getMetadata().getName());
            return deleteLater(resource);
        });
    }

    public DoneableKafkaUser tlsUser(String clusterName, String name) {
        return user(new KafkaUserBuilder().withMetadata(
                new ObjectMetaBuilder()
                        .withClusterName(clusterName)
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

    public DoneableKafkaUser scramShaUser(String clusterName, String name) {
        return user(new KafkaUserBuilder().withMetadata(
                new ObjectMetaBuilder()
                        .withClusterName(clusterName)
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

    public DoneableKafkaUser basicUser(String clusterName, String name) {
        return user(new KafkaUserBuilder()
                .withMetadata(
                    new ObjectMetaBuilder()
                        .withClusterName(clusterName)
                        .withName(name)
                        .withNamespace(client().getNamespace())
                        .addToLabels("strimzi.io/cluster", clusterName)
                        .build())
                .build());
    }

    DoneableKafkaUser user(KafkaUser user) {
        return new DoneableKafkaUser(user, ku -> {
            KafkaUser resource = kafkaUser().inNamespace(client().getNamespace()).createOrReplace(ku);
            LOGGER.info("Created KafkaUser {}", resource.getMetadata().getName());
            return deleteLater(resource);
        });
    }

    private Kafka getKafkaFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Kafka.class);
    }
    private KafkaConnect getKafkaConnectFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnect.class);
    }

    private Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }

    public DoneableDeployment clusterOperator(String namespace) {
        return clusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }

    public DoneableDeployment clusterOperator(String namespace, long operationTimeout) {
        return createNewDeployment(defaultCLusterOperator(namespace, operationTimeout).build());
    }

    private DeploymentBuilder defaultCLusterOperator(String namespace, long operationTimeout) {

        Deployment clusterOperator = getDeploymentFromYaml(STRIMZI_PATH_TO_CO_CONFIG);

        // Get env from config file
        List<EnvVar> envVars = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        // Get default CO image
        String coImage = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        // Update images
        for (EnvVar envVar : envVars) {
            switch (envVar.getName()) {
                case "STRIMZI_LOG_LEVEL":
                    envVar.setValue(Environment.STRIMZI_LOG_LEVEL);
                    break;
                case "STRIMZI_NAMESPACE":
                    envVar.setValue(namespace);
                    envVar.setValueFrom(null);
                    break;
                case "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS":
                    envVar.setValue(Environment.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
                    break;
                case "STRIMZI_OPERATION_TIMEOUT_MS":
                    envVar.setValue(Long.toString(operationTimeout));
                    break;
                default:
                    if (envVar.getName().contains("KAFKA_BRIDGE_IMAGE")) {
                        envVar.setValue(envVar.getValue());
                    } else if (envVar.getName().contains("STRIMZI_DEFAULT")) {
                        envVar.setValue(StUtils.changeOrgAndTag(envVar.getValue()));
                    } else if (envVar.getName().contains("IMAGES")) {
                        envVar.setValue(StUtils.changeOrgAndTagInImageMap(envVar.getValue()));
                    }
            }
        }

        envVars.add(new EnvVar("STRIMZI_IMAGE_PULL_POLICY", Environment.IMAGE_PULL_POLICY, null));
        // Apply updated env variables
        clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars);

        return new DeploymentBuilder(clusterOperator)
                .editSpec()
                    .withNewSelector()
                        .addToMatchLabels("name", Constants.STRIMZI_DEPLOYMENT_NAME)
                    .endSelector()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .withImage(StUtils.changeOrgAndTag(coImage))
                                .withImagePullPolicy(Environment.IMAGE_PULL_POLICY)
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec();
    }

    private DoneableDeployment createNewDeployment(Deployment deployment) {
        return new DoneableDeployment(deployment, co -> {
            TestUtils.waitFor("Deployment creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        client().createOrReplaceDeployment(co);
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

    private RoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, RoleBinding.class);
    }

    private ClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, ClusterRoleBinding.class);
    }

    DoneableRoleBinding roleBinding(String yamlPath, String namespace, String clientNamespace) {
        return roleBinding(defaultRoleBinding(yamlPath, namespace).build(), clientNamespace);
    }

    private RoleBindingBuilder defaultRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating RoleBinding from {} in namespace {}", yamlPath, namespace);

        return new RoleBindingBuilder(getRoleBindingFromYaml(yamlPath))
                .withApiVersion("rbac.authorization.k8s.io/v1")
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject();
    }

    private DoneableRoleBinding roleBinding(RoleBinding roleBinding, String clientNamespace) {
        LOGGER.info("Apply RoleBinding in namespace {}", clientNamespace);
        client().namespace(clientNamespace).createOrReplaceRoleBinding(roleBinding);
        deleteLater(roleBinding);
        return new DoneableRoleBinding(roleBinding);
    }

    DoneableClusterRoleBinding clusterRoleBinding(String yamlPath, String namespace, String clientNamespace) {
        return clusterRoleBinding(defaultClusterRoleBinding(yamlPath, namespace).build(), clientNamespace);
    }

    private ClusterRoleBindingBuilder defaultClusterRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating ClusterRoleBinding from {} in namespace {}", yamlPath, namespace);

        return new ClusterRoleBindingBuilder(getClusterRoleBindingFromYaml(yamlPath))
                .withApiVersion("rbac.authorization.k8s.io/v1")
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject();
    }

    List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespace) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");

        List<ClusterRoleBinding> kCRBList = new ArrayList<>();

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-cluster-operator-namespaced")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-cluster-operator-namespaced")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-entity-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-entity-operator")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-topic-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-topic-operator")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );
        return kCRBList;
    }

    DoneableClusterRoleBinding clusterRoleBinding(ClusterRoleBinding clusterRoleBinding, String clientNamespace) {
        LOGGER.info("Apply ClusterRoleBinding in namespace {}", clientNamespace);
        client().createOrReplaceClusterRoleBinding(clusterRoleBinding);
        deleteLater(clusterRoleBinding);
        return new DoneableClusterRoleBinding(clusterRoleBinding);
    }

    public DoneableDeployment deployKafkaClients(String kafkaClientsName) {
        return deployKafkaClients(false, kafkaClientsName, null);
    }

    DoneableDeployment deployKafkaClients(boolean tlsListener, String kafkaClientsName) {
        return deployKafkaClients(tlsListener, kafkaClientsName, null);
    }

    public DoneableDeployment deployKafkaClients(boolean tlsListener, String kafkaClientsName, KafkaUser... kafkaUsers) {
        Deployment kafkaClient = new DeploymentBuilder()
            .withNewMetadata()
                .withName(kafkaClientsName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                .addToMatchLabels("app", kafkaClientsName)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .addToLabels("app", kafkaClientsName)
                    .endMetadata()
                    .withSpec(createClientSpec(tlsListener, kafkaClientsName, kafkaUsers))
                .endTemplate()
            .endSpec()
            .build();

        return createNewDeployment(kafkaClient);
    }

    public static ServiceBuilder getSystemtestsServiceResource(String appName, int port, String namespace) {
        return new ServiceBuilder()
            .withNewMetadata()
                .withName(appName)
                .withNamespace(namespace)
                .addToLabels("run", appName)
            .endMetadata()
            .withNewSpec()
                .withSelector(Collections.singletonMap("app", appName))
                .addNewPort()
                    .withName("http")
                    .withPort(port)
                    .withProtocol("TCP")
                .endPort()
            .endSpec();
    }

    public DoneableService createServiceResource(String appName, int port, String clientNamespace) {
        Service service = getSystemtestsServiceResource(appName, port, clientNamespace).build();
        LOGGER.info("Creating service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        client().createService(service);
        deleteLater(service);
        return new DoneableService(service);
    }

    public DoneableService createServiceResource(Service service, String clientNamespace) {
        LOGGER.info("Creating service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        client().createService(service);
        deleteLater(service);
        return new DoneableService(service);
    }

    private static Ingress getSystemtestIngressResource(String appName, int port, String url) throws MalformedURLException {
        IngressBackend backend = new IngressBackend();
        backend.setServiceName(appName);
        backend.setServicePort(new IntOrString(port));
        HTTPIngressPath path = new HTTPIngressPath();
        path.setPath("/");
        path.setBackend(backend);

        return new IngressBuilder()
                .withNewMetadata()
                .withName(appName)
                .addToLabels("route", appName)
                .endMetadata()
                .withNewSpec()
                .withRules(new IngressRuleBuilder()
                        .withHost(appName + "." +  (Environment.KUBERNETES_DOMAIN.equals(".nip.io") ?  new URL(url).getHost() + ".nip.io" : Environment.KUBERNETES_DOMAIN))
                        .withNewHttp()
                        .withPaths(path)
                        .endHttp()
                        .build())
                .endSpec()
                .build();
    }

    DoneableIngress createIngress(String appName, int port, String url, String clientNamespace) throws Exception {
        Ingress ingress = getSystemtestIngressResource(appName, port, url);
        LOGGER.info("Creating ingress {} in namespace {}", ingress.getMetadata().getName(), clientNamespace);
        client().createIngress(ingress);
        deleteLater(ingress);
        return new DoneableIngress(ingress);
    }

    private PodSpec createClientSpec(boolean tlsListener, String kafkaClientsName, KafkaUser... kafkaUsers) {
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();
        ContainerBuilder containerBuilder = new ContainerBuilder()
                .withName(kafkaClientsName)
                .withImage(Environment.TEST_CLIENT_IMAGE)
                .withCommand("sleep")
                .withArgs("infinity")
                .withImagePullPolicy(Environment.IMAGE_PULL_POLICY);

        if (kafkaUsers == null) {
            String producerConfiguration = "acks=all\n";
            String consumerConfiguration = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest\n";

            containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();
            containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

        } else {
            for (KafkaUser kafkaUser : kafkaUsers) {
                String kafkaUserName = kafkaUser.getMetadata().getName();
                boolean tlsUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication;
                boolean scramShaUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication;

                String producerConfiguration = "acks=all\n";
                String consumerConfiguration = "auto.offset.reset=earliest\n";
                containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();
                containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

                String envVariablesSuffix = String.format("_%s", kafkaUserName.replace("-", "_"));
                containerBuilder.addNewEnv().withName("KAFKA_USER" + envVariablesSuffix).withValue(kafkaUserName).endEnv();

                if (tlsListener) {
                    if (scramShaUser) {
                        producerConfiguration += "security.protocol=SASL_SSL\n";
                        producerConfiguration += saslConfigs(kafkaUser);
                        consumerConfiguration += "security.protocol=SASL_SSL\n";
                        consumerConfiguration += saslConfigs(kafkaUser);
                    } else {
                        producerConfiguration += "security.protocol=SSL\n";
                        consumerConfiguration += "security.protocol=SSL\n";
                    }
                    producerConfiguration +=
                            "ssl.truststore.location=/tmp/" + kafkaUserName + "-truststore.p12\n" +
                                    "ssl.truststore.type=pkcs12\n";
                    consumerConfiguration += ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest\n" +
                            "ssl.truststore.location=/tmp/" + kafkaUserName + "-truststore.p12\n" +
                            "ssl.truststore.type=pkcs12\n";
                } else {
                    if (scramShaUser) {
                        producerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                        producerConfiguration += saslConfigs(kafkaUser);
                        consumerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                        consumerConfiguration += saslConfigs(kafkaUser);
                    } else {
                        producerConfiguration += "security.protocol=PLAINTEXT\n";
                        consumerConfiguration += "security.protocol=PLAINTEXT\n";
                    }
                }

                if (tlsUser) {
                    producerConfiguration +=
                            "ssl.keystore.location=/tmp/" + kafkaUserName + "-keystore.p12\n" +
                                    "ssl.keystore.type=pkcs12\n";
                    consumerConfiguration += "auto.offset.reset=earliest\n" +
                            "ssl.keystore.location=/tmp/" + kafkaUserName + "-keystore.p12\n" +
                            "ssl.keystore.type=pkcs12\n";

                    containerBuilder.addNewEnv().withName("PRODUCER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv()
                            .addNewEnv().withName("CONSUMER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv();

                    String userSecretVolumeName = "tls-cert-" + kafkaUserName;
                    String userSecretMountPoint = "/opt/kafka/user-secret-" + kafkaUserName;

                    containerBuilder.addNewVolumeMount()
                            .withName(userSecretVolumeName)
                            .withMountPath(userSecretMountPoint)
                            .endVolumeMount()
                            .addNewEnv().withName("USER_LOCATION" + envVariablesSuffix).withValue(userSecretMountPoint).endEnv();

                    podSpecBuilder.addNewVolume()
                            .withName(userSecretVolumeName)
                            .withNewSecret()
                            .withSecretName(kafkaUserName)
                            .endSecret()
                            .endVolume();
                }

                if (tlsListener) {
                    String clusterName = kafkaUser.getMetadata().getLabels().get("strimzi.io/cluster");
                    String clusterCaSecretName = clusterCaCertSecretName(clusterName);
                    String clusterCaSecretVolumeName = "ca-cert-" + kafkaUserName;
                    String caSecretMountPoint = "/opt/kafka/cluster-ca-" + kafkaUserName;

                    containerBuilder.addNewVolumeMount()
                            .withName(clusterCaSecretVolumeName)
                            .withMountPath(caSecretMountPoint)
                            .endVolumeMount()
                            .addNewEnv().withName("PRODUCER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv()
                            .addNewEnv().withName("CONSUMER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv()
                            .addNewEnv().withName("CA_LOCATION" + envVariablesSuffix).withValue(caSecretMountPoint).endEnv()
                            .addNewEnv().withName("TRUSTSTORE_LOCATION" + envVariablesSuffix).withValue("/tmp/"  + kafkaUserName + "-truststore.p12").endEnv();

                    if (tlsUser) {
                        containerBuilder.addNewEnv().withName("KEYSTORE_LOCATION" + envVariablesSuffix).withValue("/tmp/" + kafkaUserName + "-keystore.p12").endEnv();
                    }

                    podSpecBuilder.addNewVolume()
                            .withName(clusterCaSecretVolumeName)
                            .withNewSecret()
                            .withSecretName(clusterCaSecretName)
                            .endSecret()
                            .endVolume();
                }

                containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION" + envVariablesSuffix).withValue(producerConfiguration).endEnv();
                containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION"  + envVariablesSuffix).withValue(consumerConfiguration).endEnv();
            }
        }
        return podSpecBuilder.withContainers(containerBuilder.build()).build();
    }


    String clusterCaCertSecretName(String cluster) {
        return cluster + "-cluster-ca-cert";
    }

    String saslConfigs(KafkaUser kafkaUser) {
        Secret secret = client().getSecret(kafkaUser.getMetadata().getName());

        String password = new String(Base64.getDecoder().decode(secret.getData().get("password")), Charset.forName("UTF-8"));
        if (password.isEmpty()) {
            LOGGER.info("Secret {}:\n{}", kafkaUser.getMetadata().getName(), toYamlString(secret));
            throw new RuntimeException("The Secret " + kafkaUser.getMetadata().getName() + " lacks the 'password' key");
        }
        return "sasl.mechanism=SCRAM-SHA-512\n" +
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n" +
                "username=\"" + kafkaUser.getMetadata().getName() + "\" \\\n" +
                "password=\"" + password + "\";\n";
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

    public DoneableKafkaBridge kafkaBridge(String name, String bootstrap, int kafkaBridgeReplicas, int port) {
        return kafkaBridge(defaultKafkaBridge(name, bootstrap, kafkaBridgeReplicas, port).build());
    }

    private KafkaBridgeBuilder defaultKafkaBridge(String name, String bootstrap, int replicas, int port) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace(client().getNamespace()).build())
                .withNewSpec()
                .withBootstrapServers(bootstrap)
                .withReplicas(replicas)
                .withNewHttp(port)
                .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("memory", new Quantity("1G")).build())
                .withMetrics(new HashMap<>())
                .endSpec();
    }

    private DoneableKafkaBridge kafkaBridge(KafkaBridge kafkaBridge) {
        return new DoneableKafkaBridge(kafkaBridge, kB -> {
            TestUtils.waitFor("KafkaBridge creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        kafkaBridge().inNamespace(client().getNamespace()).createOrReplace(kB);
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
                    kB));
        });
    }

    public DoneableDeployment consumerWithTracing(String bootstrapServer) {
        String consumerName = "hello-world-consumer";

        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);

        return createNewDeployment(new DeploymentBuilder()
                    .withNewMetadata()
                        .withNamespace(client().getNamespace())
                        .withLabels(consumerLabels)
                        .withName(consumerName)
                    .endMetadata()
                    .withNewSpec()
                        .withNewSelector()
                            .withMatchLabels(consumerLabels)
                        .endSelector()
                        .withReplicas(1)
                        .withNewTemplate()
                            .withNewMetadata()
                                .withLabels(consumerLabels)
                            .endMetadata()
                            .withNewSpec()
                                .withContainers()
                                .addNewContainer()
                                    .withName(consumerName)
                                    .withImage("strimzi/" + consumerName + ":latest")
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapServer)
                                      .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue("my-topic")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("GROUP_ID")
                                        .withValue("my-" + consumerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("DELAY_MS")
                                        .withValue("1000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("INFO")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MESSAGE_COUNT")
                                        .withValue("1000000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SERVICE_NAME")
                                        .withValue(consumerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_AGENT_HOST")
                                        .withValue("my-jaeger-agent")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_TYPE")
                                        .withValue("const")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_PARAM")
                                        .withValue("1")
                                    .endEnv()
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build());
    }

    public DoneableDeployment producerWithTracing(String bootstrapServer) {
        String producerName = "hello-world-producer";

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);

        return createNewDeployment(new DeploymentBuilder()
                    .withNewMetadata()
                        .withNamespace(client().getNamespace())
                        .withLabels(producerLabels)
                        .withName(producerName)
                    .endMetadata()
                    .withNewSpec()
                        .withNewSelector()
                            .withMatchLabels(producerLabels)
                        .endSelector()
                        .withReplicas(1)
                        .withNewTemplate()
                            .withNewMetadata()
                                .withLabels(producerLabels)
                            .endMetadata()
                            .withNewSpec()
                                .withContainers()
                                .addNewContainer()
                                    .withName(producerName)
                                    .withImage("strimzi/" + producerName + ":latest")
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapServer)
                                      .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue("my-topic")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("DELAY_MS")
                                        .withValue("1000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("INFO")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MESSAGE_COUNT")
                                        .withValue("1000000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SERVICE_NAME")
                                        .withValue(producerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_AGENT_HOST")
                                        .withValue("my-jaeger-agent")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_TYPE")
                                        .withValue("const")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_PARAM")
                                        .withValue("1")
                                    .endEnv()
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build());
    }

    public DoneableDeployment kafkaStreamsWithTracing(String bootstrapServer) {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamLabels = new HashMap<>();
        kafkaStreamLabels.put("app", kafkaStreamsName);

        return createNewDeployment(new DeploymentBuilder()
                    .withNewMetadata()
                        .withNamespace(client().getNamespace())
                        .withLabels(kafkaStreamLabels)
                        .withName(kafkaStreamsName)
                    .endMetadata()
                    .withNewSpec()
                        .withNewSelector()
                            .withMatchLabels(kafkaStreamLabels)
                        .endSelector()
                        .withReplicas(1)
                        .withNewTemplate()
                            .withNewMetadata()
                                .withLabels(kafkaStreamLabels)
                            .endMetadata()
                            .withNewSpec()
                                .withContainers()
                                .addNewContainer()
                                    .withName(kafkaStreamsName)
                                    .withImage("strimzi/" + kafkaStreamsName + ":latest")
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapServer)
                                      .endEnv()
                                    .addNewEnv()
                                        .withName("APPLICATION_ID")
                                        .withValue(kafkaStreamsName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("SOURCE_TOPIC")
                                        .withValue("my-topic")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TARGET_TOPIC")
                                        .withValue("cipot-ym")
                                    .endEnv()
                                      .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("INFO")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SERVICE_NAME")
                                        .withValue(kafkaStreamsName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_AGENT_HOST")
                                        .withValue("my-jaeger-agent")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_TYPE")
                                        .withValue("const")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_PARAM")
                                        .withValue("1")
                                    .endEnv()
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build());
    }

    public DoneableDeployment deployKeycloak() {
        String keycloakName = "keycloak";

        Map<String, String> keycloakLabels = new HashMap<>();
        keycloakLabels.put("app", keycloakName);

        return createNewDeployment(new DeploymentBuilder()
                .withNewMetadata()
                    .withNamespace(client().getNamespace())
                    .withLabels(keycloakLabels)
                    .withName(keycloakName)
                .endMetadata()
                .withNewSpec()
                    .withNewSelector()
                        .withMatchLabels(keycloakLabels)
                    .endSelector()
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(keycloakLabels)
                        .endMetadata()
                        .withNewSpec()
                            .withContainers()
                            .addNewContainer()
                                .withName(keycloakName + "pod")
                                .withImage("jboss/keycloak")
                                .withPorts(
                                        new ContainerPortBuilder()
                                                .withName("http")
                                                .withContainerPort(8080)
                                                .build(),
                                        new ContainerPortBuilder()
                                                .withName("https")
                                                .withContainerPort(8443)
                                                .build()
                                )
                                .addNewEnv()
                                    .withName("KEYCLOAK_USER")
                                    .withValue("admin")
                                .endEnv()
                                .addNewEnv()
                                    .withName("KEYCLOAK_PASSWORD")
                                    .withValue("admin")
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build());
    }

    public Service deployKeycloakNodePortService(String namespace) {
        String keycloakName = "keycloak";

        Map<String, String> keycloakLabels = new HashMap<>();
        keycloakLabels.put("app", keycloakName);

        return getSystemtestsServiceResource(keycloakName + "service-https",
                Constants.HTTPS_KEYCLOAK_DEFAULT_PORT, namespace)
                    .editSpec()
                        .withType("NodePort")
                        .withSelector(keycloakLabels)
                        .editFirstPort()
                            .withNodePort(32223)
                        .endPort()
                    .endSpec().build();
    }

    public Service deployBridgeNodePortService(String bridgeExternalService, String namespace) {
        Map<String, String> map = new HashMap<>();
        map.put("strimzi.io/cluster", CLUSTER_NAME);
        map.put("strimzi.io/kind", "KafkaBridge");
        map.put("strimzi.io/name", CLUSTER_NAME + "-bridge");

        // Create node port service for expose bridge outside the cluster
        return getSystemtestsServiceResource(bridgeExternalService, Constants.HTTP_BRIDGE_DEFAULT_PORT, namespace)
                .editSpec()
                .withType("NodePort")
                .withSelector(map)
                .endSpec().build();
    }

    public DoneableDeployment producerWithOauth(String oauthTokenEndpointUri, String topicName, String bootstrap) {
        String producerName = "hello-world-producer";

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);

        return createNewDeployment(new DeploymentBuilder()
                .withNewMetadata()
                        .withNamespace(client().getNamespace())
                        .withClusterName(client().getNamespace())
                        .withLabels(producerLabels)
                        .withName("hello-world-producer")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withNewSelector()
                        .withMatchLabels(producerLabels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(producerLabels)
                        .endMetadata()
                        .withNewSpec()
                            .withContainers()
                            .addNewContainer()
                                .withName(producerName)
                                .withImage("strimzi/" + producerName + ":latest")
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrap)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("DELAY_MS")
                                    .withValue("1000")
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue("100")
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CLIENT_ID")
                                    .withValue("hello-world-producer")
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CLIENT_SECRET")
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef()
                                            .withName("hello-world-producer-oauth")
                                            .withKey("clientSecret")
                                        .endSecretKeyRef()
                                    .endValueFrom()
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                    .withValue(oauthTokenEndpointUri)
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CRT")
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef()
                                            .withName("x509-https-secret")
                                            .withKey("tls.crt")
                                        .endSecretKeyRef()
                                    .endValueFrom()
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM") // disable hostname verification
                                    .withValue("")
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec().build());
    }


    public DoneableDeployment consumerWithOauth(String oauthTokenEndpointUri, String topicName, String bootstrap) {
        return consumerWithOauth("hello-world-consumer", oauthTokenEndpointUri, topicName, bootstrap);
    }

    public DoneableDeployment consumerWithOauth(String name, String oauthTokenEndpointUri, String topicName, String bootstrap) {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", name);

        return createNewDeployment(new DeploymentBuilder()
                .withNewMetadata()
                        .withNamespace(client().getNamespace())
                        .withClusterName(client().getNamespace())
                        .withLabels(consumerLabels)
                        .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withNewSelector()
                        .withMatchLabels(consumerLabels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(consumerLabels)
                        .endMetadata()
                        .withNewSpec()
                            .withContainers()
                            .addNewContainer()
                                .withName(name)
                                .withImage("strimzi/hello-world-consumer:latest")
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrap)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("GROUP_ID")
                                    .withValue(name)
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("INFO")
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue("100")
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CLIENT_ID")
                                    .withValue("hello-world-consumer")
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CLIENT_SECRET")
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef()
                                            .withName("hello-world-consumer-oauth")
                                            .withKey("clientSecret")
                                        .endSecretKeyRef()
                                    .endValueFrom()
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                    .withValue(oauthTokenEndpointUri)
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CRT")
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef()
                                            .withName("x509-https-secret")
                                            .withKey("tls.crt")
                                        .endSecretKeyRef()
                                    .endValueFrom()
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM")  // disable hostname verification
                                    .withValue("")
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec().build());
    }

    public DoneableDeployment kafkaStreamsWithOauth(String oauthTokenEndpointUri, String bootstrap) {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamsLabel = new HashMap<>();
        kafkaStreamsLabel.put("app", kafkaStreamsName);

        return createNewDeployment(new DeploymentBuilder()
                .withNewMetadata()
                    .withNamespace(client().getNamespace())
                    .withClusterName(client().getNamespace())
                    .withLabels(kafkaStreamsLabel)
                    .withName(kafkaStreamsName)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withNewSelector()
                        .withMatchLabels(kafkaStreamsLabel)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(kafkaStreamsLabel)
                        .endMetadata()
                        .withNewSpec()
                        .withContainers()
                            .addNewContainer()
                                .withName(kafkaStreamsName)
                                .withImage("strimzi/" + kafkaStreamsName + ":latest")
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrap)
                                .endEnv()
                                .addNewEnv()
                                    .withName("APPLICATION_ID")
                                    .withValue(kafkaStreamsName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("SOURCE_TOPIC")
                                    .withValue("my-topic")
                                .endEnv()
                                .addNewEnv()
                                    .withName("TARGET_TOPIC")
                                    .withValue("my-topic-reversed")
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CLIENT_ID")
                                    .withValue(kafkaStreamsName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CLIENT_SECRET")
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef()
                                            .withName("hello-world-streams-oauth")
                                            .withKey("clientSecret")
                                        .endSecretKeyRef()
                                    .endValueFrom()
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                    .withValue(oauthTokenEndpointUri)
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_CRT")
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef()
                                            .withName("x509-https-secret")
                                            .withKey("tls.crt")
                                        .endSecretKeyRef()
                                    .endValueFrom()
                                .endEnv()
                                .addNewEnv()
                                    .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM") // disable hostname verification
                                    .withValue("")
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec().build());
    }
}
