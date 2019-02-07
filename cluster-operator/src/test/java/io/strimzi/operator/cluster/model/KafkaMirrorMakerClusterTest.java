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
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpecBuilder;
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
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaMirrorMakerClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
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
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;

    private KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
            .withBootstrapServers(producerBootstrapServers)
            .withConfig((Map<String, Object>) TestUtils.fromJson(producerConfigurationJson, Map.class))
            .build();
    private KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
            .withBootstrapServers(consumerBootstrapServers)
            .withGroupId(groupId)
            .withNumStreams(numStreams)
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
        return expectedLabels(mm.kafkaMirrorMakerClusterName(cluster));
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
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(KafkaMirrorMakerCluster.DEFAULT_KAFKA_GC_LOG_ENABLED).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
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
        Deployment dep = mm.generateDeployment(new HashMap<String, String>(), true, null);

        assertEquals(mm.kafkaMirrorMakerClusterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = expectedLabels();
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), dep.getSpec().getSelector().getMatchLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(mm.kafkaMirrorMakerClusterName(this.cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
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
        Deployment dep = kc.generateDeployment(emptyMap(), true, null);

        assertEquals("my-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
        assertEquals("my-another-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "my-secret-p",
                containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "my-another-secret-p",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("my-secret-p/cert.crt;my-secret-p/new-cert.crt;my-another-secret-p/another-cert.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER));

        assertEquals("my-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName());
        assertEquals("my-another-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(4).getName());

        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "my-secret-c",
                containers.get(0).getVolumeMounts().get(3).getMountPath());
        assertEquals(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "my-another-secret-c",
                containers.get(0).getVolumeMounts().get(4).getMountPath());

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
                        new KafkaMirrorMakerAuthenticationTlsBuilder()
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
                        new KafkaMirrorMakerAuthenticationTlsBuilder()
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
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null);

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
                        new KafkaMirrorMakerAuthenticationTlsBuilder()
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
                        new KafkaMirrorMakerAuthenticationTlsBuilder()
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
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null);

        assertEquals(3, dep.getSpec().getTemplate().getSpec().getVolumes().size());
        assertEquals("my-secret-p", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
        assertEquals("my-secret-c", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());
    }

    @Test
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewProducer()
                    .withNewKafkaMirrorMakerAuthenticationScramSha512()
                        .withUsername("producer")
                        .withNewPasswordSecret()
                            .withSecretName("producer-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaMirrorMakerAuthenticationScramSha512()
                .endProducer()
                .editOrNewConsumer()
                    .withNewKafkaMirrorMakerAuthenticationScramSha512()
                        .withUsername("consumer")
                        .withNewPasswordSecret()
                            .withSecretName("consumer-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaMirrorMakerAuthenticationScramSha512()
                .endConsumer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null);

        assertEquals("producer-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_PRODUCER + "producer-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());

        assertEquals("producer-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER));
        assertEquals("producer",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER));

        assertEquals("consumer-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());


        assertEquals(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_CONSUMER + "consumer-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

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
        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
        assertTrue(dep.getMetadata().getLabels().entrySet().containsAll(depLabels.entrySet()));
        assertTrue(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()));

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

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testDefaultGracePeriod() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
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

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
        assertEquals(0, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
    }

    @Test
    public void testSecurityContext() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withNewRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
        assertNotNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup());
        assertEquals(Long.valueOf(456), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup());
        assertEquals(Long.valueOf(789), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser());
    }

    @Test
    public void testDefaultSecurityContext() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null);
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
}
