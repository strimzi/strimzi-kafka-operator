/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaMirrorMakerClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef 2.0.x"),
            emptyMap(), emptyMap(), emptyMap(),
            singletonMap("2.0.0", "strimzi/kafka-mirror-maker:latest-kafka-2.0.0")) { };
    private final String namespace = "test";
    private final String cluster = "mirror";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String producerConfigurationJson = "{\"foo\":\"bar\"}";
    private final String consumerConfigurationJson = "{\"foo\":\"buz\"}";
    private final String defaultProducerConfiguration = "";
    private final String defaultConsumerConfiguration = "";
    private final String expectedProducerConfiguration = "foo=bar" + LINE_SEPARATOR;
    private final String expectedConsumerConfiguration = "foo=buz" + LINE_SEPARATOR;
    private final String producerBootstrapServers = "target-kafka:9092";
    private final String consumerBootstrapServers = "source-kafka:9092";
    private final String groupId = "my-group-id";
    private final int numStreams = 2;
    private final String whitelist = ".*";
    private final int offsetCommitInterval = 42000;
    private final boolean abortOnSendFailure = false;
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;

    private KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
            .withBootstrapServers(producerBootstrapServers)
            .withAbortOnSendFailure(abortOnSendFailure)
            .withConfig((Map<String, Object>) TestUtils.fromJson(producerConfigurationJson, Map.class))
            .build();
    private KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
            .withBootstrapServers(consumerBootstrapServers)
            .withGroupId(groupId)
            .withNumStreams(numStreams)
            .withOffsetCommitInterval(offsetCommitInterval)
            .withConfig((Map<String, Object>) TestUtils.fromJson(consumerConfigurationJson, Map.class))
            .build();
    private final KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(ResourceUtils.createEmptyKafkaMirrorMakerCluster(namespace, cluster))
            .withNewSpec()
            .withImage(image)
            .withReplicas(replicas)
            .withProducer(producer)
            .withConsumer(consumer)
            .withWhitelist(whitelist)
            .withMetrics((Map<String, Object>) TestUtils.fromJson(metricsCmJson, Map.class))
            .endSpec()
            .build();

    private final KafkaMirrorMakerCluster mm = KafkaMirrorMakerCluster.fromCrd(resource,
            VERSIONS);

    @Rule
    public ResourceTester<KafkaMirrorMaker, KafkaMirrorMakerCluster> resourceTester = new ResourceTester<>(KafkaMirrorMaker.class, VERSIONS, KafkaMirrorMakerCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = mm.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaMirrorMakerResources.deploymentName(cluster));
    }

    protected List<EnvVar> getExpectedEnvVars() {

        List<EnvVar> expected = new ArrayList<>();

        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER).withValue(expectedConsumerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER).withValue(expectedProducerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_METRICS_ENABLED).withValue("true").build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER).withValue(consumerBootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER).withValue(producerBootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_WHITELIST).withValue(whitelist).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_GROUPID_CONSUMER).withValue(groupId).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_NUMSTREAMS).withValue(Integer.toString(numStreams)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL).withValue(Integer.toString(offsetCommitInterval)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE).withValue(Boolean.toString(abortOnSendFailure)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(KafkaMirrorMakerCluster.DEFAULT_KAFKA_GC_LOG_ENABLED).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_LIVENESS_PERIOD).withValue("10").build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_READINESS_PERIOD).withValue("10").build());

        return expected;
    }

    @Test
    public void testDefaultValues() {
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(ResourceUtils.createEmptyKafkaMirrorMakerCluster(namespace, cluster))
                .withNewSpec()
                    .withReplicas(replicas)
                    .withProducer(producer)
                    .withConsumer(consumer)
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mm = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        assertEquals("strimzi/kafka-mirror-maker:latest-kafka-2.0.0", mm.image);
        assertEquals(defaultConsumerConfiguration,
                new KafkaMirrorMakerConsumerConfiguration(mm.consumer.getConfig().entrySet()).getConfiguration());
        assertEquals(defaultProducerConfiguration,
                new KafkaMirrorMakerProducerConfiguration(mm.producer.getConfig().entrySet()).getConfiguration());
    }

    @Test
    public void testFromCrd() {
        assertEquals(replicas, mm.getReplicas());
        assertEquals(image, mm.getImage());
        assertEquals(consumerBootstrapServers, mm.consumer.getBootstrapServers());
        assertEquals(producerBootstrapServers, mm.producer.getBootstrapServers());
        assertEquals(whitelist, mm.getWhitelist());
        assertEquals(groupId, mm.consumer.getGroupId());
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), mm.getEnvVars());
    }

    @Test
    public void testGenerateDeployment()   {
        Deployment dep = mm.generateDeployment(new HashMap<String, String>(), true, null, null);

        assertEquals(KafkaMirrorMakerResources.deploymentName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = expectedLabels();
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), dep.getSpec().getSelector().getMatchLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(KafkaMirrorMakerResources.deploymentName(this.cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(mm.image, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        checkOwnerReference(mm.createOwnerReference(), dep);
    }

    @Test
    public void testGenerateDeploymentWithTls() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewConsumer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret-c").withCertificate("another-cert.crt").build())
                .endTls()
                .endConsumer()
                .editOrNewProducer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret-p").withCertificate("another-cert.crt").build())
                .endTls()
                .endProducer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("my-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
        assertEquals("my-another-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "my-secret-p",
                containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "my-another-secret-p",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("true",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_PRODUCER));
        assertEquals("my-secret-p/cert.crt;my-secret-p/new-cert.crt;my-another-secret-p/another-cert.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER));

        assertEquals("my-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName());
        assertEquals("my-another-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(4).getName());

        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "my-secret-c",
                containers.get(0).getVolumeMounts().get(3).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "my-another-secret-c",
                containers.get(0).getVolumeMounts().get(4).getMountPath());

        assertEquals("true",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_CONSUMER));
        assertEquals("my-secret-c/cert.crt;my-secret-c/new-cert.crt;my-another-secret-c/another-cert.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER));
    }

    @Test
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewConsumer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret-c")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endConsumer()
                .editOrNewProducer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret-p")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endProducer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("user-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(4).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "user-secret-c",
                containers.get(0).getVolumeMounts().get(4).getMountPath());

        assertEquals("user-secret-c/user.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER));
        assertEquals("user-secret-c/user.key",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER));

        assertEquals("user-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "user-secret-p",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("user-secret-p/user.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER));
        assertEquals("user-secret-p/user.key",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER));
    }

    @Test
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewConsumer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret-c")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endConsumer()
                .editOrNewProducer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret-p")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endProducer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertEquals(3, dep.getSpec().getTemplate().getSpec().getVolumes().size());
        assertEquals("my-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
        assertEquals("my-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());
    }

    @Test
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewProducer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("producer")
                        .withNewPasswordSecret()
                            .withSecretName("producer-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endProducer()
                .editOrNewConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("consumer")
                        .withNewPasswordSecret()
                            .withSecretName("consumer-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endConsumer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("producer-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_PRODUCER + "producer-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());

        assertEquals("scram-sha-512",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER));
        assertEquals("producer-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER));
        assertEquals("producer",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER));

        assertEquals("consumer-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());


        assertEquals(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_CONSUMER + "consumer-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("scram-sha-512",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER));
        assertEquals("consumer-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER));
        assertEquals("consumer",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER));
    }

    @Test
    public void testGenerateDeploymentWithPlain() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editOrNewProducer()
                        .withNewKafkaClientAuthenticationPlain()
                            .withUsername("producer")
                            .withNewPasswordSecret()
                                .withSecretName("producer-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationPlain()
                    .endProducer()
                    .editOrNewConsumer()
                        .withNewKafkaClientAuthenticationPlain()
                            .withUsername("consumer")
                            .withNewPasswordSecret()
                                .withSecretName("consumer-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationPlain()
                    .endConsumer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("producer-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_PRODUCER + "producer-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());

        assertEquals("plain",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER));
        assertEquals("producer-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER));
        assertEquals("producer",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER));

        assertEquals("consumer-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());


        assertEquals(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_CONSUMER + "consumer-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("plain",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER));
        assertEquals("consumer-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER));
        assertEquals("consumer",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER));
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertEquals(1, resource.getMetadata().getOwnerReferences().size());
        assertEquals(ownerRef, resource.getMetadata().getOwnerReferences().get(0));
    }

    @Test
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> pdbLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> pdbAnots = TestUtils.map("a5", "v5", "a6", "v6");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewDeployment()
                            .withNewMetadata()
                                .withLabels(depLabels)
                                .withAnnotations(depAnots)
                            .endMetadata()
                        .endDeployment()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnots)
                            .endMetadata()
                            .withNewPriorityClassName("top-priority")
                        .endPod()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnots)
                            .endMetadata()
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertTrue(dep.getMetadata().getLabels().entrySet().containsAll(depLabels.entrySet()));
        assertTrue(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()));
        assertEquals("top-priority", dep.getSpec().getTemplate().getSpec().getPriorityClassName());

        // Check Pods
        assertTrue(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()));
        assertTrue(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = mmc.generatePodDisruptionBudget();
        assertTrue(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()));
        assertTrue(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()));
    }

    @Test
    public void testGracePeriod() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testDefaultGracePeriod() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(Long.valueOf(30), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsFromCo() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(this.resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, secrets);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertFalse(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(0, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
    }

    @Test
    public void testSecurityContext() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertNotNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup());
        assertEquals(Long.valueOf(456), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup());
        assertEquals(Long.valueOf(789), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser());
    }

    @Test
    public void testDefaultSecurityContext() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
    }

    @Test
    public void testPodDisruptionBudget() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        PodDisruptionBudget pdb = mmc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(2), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        PodDisruptionBudget pdb = mmc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(1), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testImagePullPolicy() {
        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.ALWAYS, null);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());

        dep = kc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.IFNOTPRESENT, null);
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
    }

    @Test
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(limits, cont.getResources().getLimits());
        assertEquals(requests, cont.getResources().getRequests());
    }

    @Test
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withNewXms("512m")
                        .withNewXmx("1024m")
                        .withNewServer(true)
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-server"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"));
    }

    @Test
    public void testDefaultProbes() {
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(this.resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        Probe livenessProbe = cont.getLivenessProbe();
        Probe readinessProbe = cont.getReadinessProbe();

        assertEquals("/opt/kafka/kafka_mirror_maker_liveness.sh", livenessProbe.getExec().getCommand().get(0));
        assertEquals(new Integer(60), livenessProbe.getInitialDelaySeconds());
        assertEquals(new Integer(5), livenessProbe.getTimeoutSeconds());

        assertEquals(3, readinessProbe.getExec().getCommand().size());
        assertEquals("test", readinessProbe.getExec().getCommand().get(0));
        assertEquals("-f", readinessProbe.getExec().getCommand().get(1));
        assertEquals("/tmp/mirror-maker-ready", readinessProbe.getExec().getCommand().get(2));
        assertEquals(new Integer(60), readinessProbe.getInitialDelaySeconds());
        assertEquals(new Integer(5), readinessProbe.getTimeoutSeconds());

        assertTrue(cont.getEnv().stream().filter(env -> "STRIMZI_READINESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("10"));
        assertTrue(cont.getEnv().stream().filter(env -> "STRIMZI_LIVENESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("10"));
    }

    @Test
    public void testConfiguredProbes() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(120)
                        .withTimeoutSeconds(10)
                        .withPeriodSeconds(60)
                    .endLivenessProbe()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(121)
                        .withTimeoutSeconds(11)
                        .withPeriodSeconds(61)
                    .endReadinessProbe()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        Probe livenessProbe = cont.getLivenessProbe();
        Probe readinessProbe = cont.getReadinessProbe();

        assertEquals("/opt/kafka/kafka_mirror_maker_liveness.sh", livenessProbe.getExec().getCommand().get(0));
        assertEquals(new Integer(120), livenessProbe.getInitialDelaySeconds());
        assertEquals(new Integer(10), livenessProbe.getTimeoutSeconds());
        assertEquals(new Integer(60), livenessProbe.getPeriodSeconds());

        assertEquals(3, readinessProbe.getExec().getCommand().size());
        assertEquals("test", readinessProbe.getExec().getCommand().get(0));
        assertEquals("-f", readinessProbe.getExec().getCommand().get(1));
        assertEquals("/tmp/mirror-maker-ready", readinessProbe.getExec().getCommand().get(2));
        assertEquals(new Integer(121), readinessProbe.getInitialDelaySeconds());
        assertEquals(new Integer(11), readinessProbe.getTimeoutSeconds());
        assertEquals(new Integer(61), readinessProbe.getPeriodSeconds());

        assertTrue(cont.getEnv().stream().filter(env -> "STRIMZI_READINESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("61"));
        assertTrue(cont.getEnv().stream().filter(env -> "STRIMZI_LIVENESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("60"));
    }

    @Test
    public void testKafkaMMContainerEnvVars() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaMMContainer = new ContainerTemplate();
        kafkaMMContainer.setEnv(testEnvs);

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withMirrorMakerContainer(kafkaMMContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS).getEnvVars();

        assertTrue("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue));
        assertTrue("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue));
    }

    @Test
    public void testKafkaMMContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_WHITELIST;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaMMContainer = new ContainerTemplate();
        kafkaMMContainer.setEnv(testEnvs);

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withMirrorMakerContainer(kafkaMMContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS).getEnvVars();

        assertFalse("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue));
        assertFalse("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue));
    }

    @Test
    public void testTracing() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertTrue(cont.getEnv().stream().filter(env -> KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("jaeger"));
        assertTrue(cont.getEnv().stream().filter(env -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("interceptor.classes=io.opentracing.contrib.kafka.TracingConsumerInterceptor"));
        assertTrue(cont.getEnv().stream().filter(env -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("interceptor.classes=io.opentracing.contrib.kafka.TracingProducerInterceptor"));
    }

    @Test
    public void testGenerateDeploymentWithConsumerOAuthWithAccessToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withNewAccessToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endAccessToken()
                                        .build())
                        .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-token-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-token-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertTrue(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue().isEmpty());
    }

    @Test
    public void testGenerateDeploymentWithConsumerOAuthWithRefreshToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewRefreshToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endRefreshToken()
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-token-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-token-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server"), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue().trim());
    }

    @Test
    public void testGenerateDeploymentWithConsumerOAuthWithClientSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server"), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue().trim());
    }

    @Test(expected = InvalidResourceException.class)
    public void testGenerateDeploymentWithConsumerOAuthWithMissingClientSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
    }

    @Test(expected = InvalidResourceException.class)
    public void testGenerateDeploymentWithConsumerOAuthWithMissingUri() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
    }

    @Test
    public void testGenerateDeploymentWithConsumerOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue().trim());

        // Volume mounts
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER, cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER, cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath());

        // Volumes
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size());
        assertEquals("ca.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey());
        assertEquals("first-certificate/ca.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath());
        assertEquals("ca2.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getKey());
        assertEquals("first-certificate/ca2.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getPath());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size());
        assertEquals("tls.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey());
        assertEquals("second-certificate/tls.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath());
    }

    @Test
    public void testGenerateDeploymentWithProducerOAuthWithAccessToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withNewAccessToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endAccessToken()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-token-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-token-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertTrue(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue().isEmpty());
    }

    @Test
    public void testGenerateDeploymentWithProducerOAuthWithRefreshToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewRefreshToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endRefreshToken()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-token-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-token-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server"), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue().trim());
    }

    @Test
    public void testGenerateDeploymentWithProducerOAuthWithClientSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server"), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue().trim());
    }

    @Test(expected = InvalidResourceException.class)
    public void testGenerateDeploymentWithProducerOAuthWithMissingClientSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
    }

    @Test(expected = InvalidResourceException.class)
    public void testGenerateDeploymentWithProducerOAuthWithMissingUri() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
    }

    @Test
    public void testGenerateDeploymentWithProducerOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue().trim());

        // Volume mounts
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER, cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER, cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath());

        // Volumes
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size());
        assertEquals("ca.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey());
        assertEquals("first-certificate/ca.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath());
        assertEquals("ca2.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getKey());
        assertEquals("first-certificate/ca2.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getPath());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size());
        assertEquals("tls.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey());
        assertEquals("second-certificate/tls.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath());
    }

    @Test
    public void testGenerateDeploymentWithBothSidedOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endConsumer()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElse(null).getValue().trim());
        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""), cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElse(null).getValue().trim());

        // Volume mounts
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER, cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName()) && KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER.equals(mount.getMountPath())).findFirst().orElse(null).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER, cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName()) && KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER.equals(mount.getMountPath())).findFirst().orElse(null).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER, cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName()) && KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER.equals(mount.getMountPath())).findFirst().orElse(null).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER, cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName()) && KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER.equals(mount.getMountPath())).findFirst().orElse(null).getMountPath());

        // Volumes
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size());
        assertEquals("ca.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey());
        assertEquals("first-certificate/ca.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath());
        assertEquals("ca2.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getKey());
        assertEquals("first-certificate/ca2.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getPath());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size());
        assertEquals("tls.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey());
        assertEquals("second-certificate/tls.crt", dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath());
    }
}
