/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.openshift.api.model.BinaryBuildSource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageChangeTrigger;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnectAuthenticationScramSha512Builder;
import io.strimzi.api.kafka.model.KafkaConnectAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSourceBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaConnectS2IClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            emptyMap(), emptyMap(), singletonMap("2.0.0", "strimzi/kafka-connect-s2i:latest-kafka-2.0.0"),
            emptyMap()) { };
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String configurationJson = "{\"foo\":\"bar\"}";
    private final String bootstrapServers = "foo-kafka:9092";
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;
    private final OrderedProperties defaultConfiguration = new OrderedProperties()
            .addPair("config.storage.topic", "connect-cluster-configs")
            .addPair("group.id", "connect-cluster")
            .addPair("status.storage.topic", "connect-cluster-status")
            .addPair("offset.storage.topic", "connect-cluster-offsets")
            .addPair("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .addPair("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    private final OrderedProperties expectedConfiguration = new OrderedProperties()
            .addMapPairs(defaultConfiguration.asMap())
            .addPair("foo", "bar");
    private final boolean insecureSourceRepo = false;


    private final KafkaConnectS2I resource = ResourceUtils.createKafkaConnectS2ICluster(namespace, cluster, replicas, image,
            healthDelay, healthTimeout, metricsCmJson, configurationJson, insecureSourceRepo, bootstrapServers);

    private final KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

    @Rule
    public ResourceTester<KafkaConnectS2I, KafkaConnectS2ICluster> resourceTester = new ResourceTester<>(KafkaConnectS2I.class,
        x -> KafkaConnectS2ICluster.fromCrd(x, VERSIONS));


    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map("my-user-label", "cromulent", Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_NAME_LABEL, name, Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(kc.kafkaConnectClusterName(cluster));
    }

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<EnvVar>();
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION).withValue(expectedConfiguration.asPairs()).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(KafkaConnectCluster.DEFAULT_KAFKA_GC_LOG_ENABLED).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        return expected;
    }

    @Test
    public void testDefaultValues() {
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(ResourceUtils.createEmptyKafkaConnectS2ICluster(namespace, cluster), VERSIONS);

        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", kc.image);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_REPLICAS, kc.replicas);
        assertEquals("strimzi/kafka-connect-s2i:latest-kafka-2.0.0", kc.sourceImageBaseName + ":" + kc.sourceImageTag);
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_DELAY, kc.readinessProbeOptions.getInitialDelaySeconds());
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.readinessProbeOptions.getTimeoutSeconds());
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_DELAY, kc.livenessProbeOptions.getInitialDelaySeconds());
        assertEquals(KafkaConnectS2ICluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.livenessProbeOptions.getTimeoutSeconds());
        assertEquals(defaultConfiguration, kc.getConfiguration().asOrderedProperties());
        assertFalse(kc.isInsecureSourceRepository());
    }

    @Test
    public void testFromCrd() {
        assertEquals(kc.kafkaConnectClusterName(cluster) + ":latest", kc.image);
        assertEquals(replicas, kc.replicas);
        assertEquals(image, kc.sourceImageBaseName + ":" + kc.sourceImageTag);
        assertEquals(healthDelay, kc.readinessProbeOptions.getInitialDelaySeconds());
        assertEquals(healthTimeout, kc.readinessProbeOptions.getTimeoutSeconds());
        assertEquals(healthDelay, kc.livenessProbeOptions.getInitialDelaySeconds());
        assertEquals(healthTimeout, kc.livenessProbeOptions.getTimeoutSeconds());
        assertEquals(expectedConfiguration, kc.getConfiguration().asOrderedProperties());
        assertEquals(bootstrapServers, kc.bootstrapServers);
        assertFalse(kc.isInsecureSourceRepository());
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), kc.getEnvVars());
    }

    @Test
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        assertEquals(expectedLabels(kc.serviceName(cluster)), svc.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), svc.getSpec().getSelector());
        assertEquals(2, svc.getSpec().getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
        assertEquals(kc.getPrometheusAnnotations(), svc.getMetadata().getAnnotations());
        checkOwnerReference(kc.createOwnerReference(), svc);
    }

    @Test
    public void testGenerateDeploymentConfig()   {
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);

        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = expectedLabels(kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), dep.getSpec().getSelector());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(kc.kafkaConnectClusterName(this.cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(kc.kafkaConnectClusterName(this.cluster) + ":latest", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());
        assertEquals(2, dep.getSpec().getTriggers().size());
        assertEquals("ConfigChange", dep.getSpec().getTriggers().get(0).getType());
        assertEquals("ImageChange", dep.getSpec().getTriggers().get(1).getType());
        assertEquals(true, dep.getSpec().getTriggers().get(1).getImageChangeParams().getAutomatic());
        assertEquals(1, dep.getSpec().getTriggers().get(1).getImageChangeParams().getContainerNames().size());
        assertEquals(kc.kafkaConnectClusterName(this.cluster), dep.getSpec().getTriggers().get(1).getImageChangeParams().getContainerNames().get(0));
        assertEquals(kc.kafkaConnectClusterName(this.cluster) + ":latest", dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getName());
        assertEquals("ImageStreamTag", dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getKind());
        assertEquals("Rolling", dep.getSpec().getStrategy().getType());
        assertEquals(new Integer(1), dep.getSpec().getStrategy().getRollingParams().getMaxSurge().getIntVal());
        assertEquals(new Integer(0), dep.getSpec().getStrategy().getRollingParams().getMaxUnavailable().getIntVal());
        assertNull(AbstractModel.containerEnvVars(dep.getSpec().getTemplate().getSpec().getContainers().get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS));
        checkOwnerReference(kc.createOwnerReference(), dep);
    }

    @Test
    public void testGenerateBuildConfig() {
        BuildConfig bc = kc.generateBuildConfig();

        assertEquals(kc.kafkaConnectClusterName(cluster), bc.getMetadata().getName());
        assertEquals(namespace, bc.getMetadata().getNamespace());
        assertEquals(expectedLabels(kc.kafkaConnectClusterName(cluster)), bc.getMetadata().getLabels());
        assertEquals("ImageStreamTag", bc.getSpec().getOutput().getTo().getKind());
        assertEquals(kc.image, bc.getSpec().getOutput().getTo().getName());
        assertEquals("Serial", bc.getSpec().getRunPolicy());
        assertEquals("Binary", bc.getSpec().getSource().getType());
        assertEquals(new BinaryBuildSource(), bc.getSpec().getSource().getBinary());
        assertEquals("Source", bc.getSpec().getStrategy().getType());
        assertEquals("ImageStreamTag", bc.getSpec().getStrategy().getSourceStrategy().getFrom().getKind());
        assertEquals(kc.getSourceImageStreamName() + ":" + kc.sourceImageTag, bc.getSpec().getStrategy().getSourceStrategy().getFrom().getName());
        assertEquals(2, bc.getSpec().getTriggers().size());
        assertEquals("ConfigChange", bc.getSpec().getTriggers().get(0).getType());
        assertEquals("ImageChange", bc.getSpec().getTriggers().get(1).getType());
        assertEquals(new ImageChangeTrigger(), bc.getSpec().getTriggers().get(1).getImageChange());
        checkOwnerReference(kc.createOwnerReference(), bc);
    }

    @Test
    public void testGenerateSourceImageStream() {
        ImageStream is = kc.generateSourceImageStream();

        assertEquals(kc.getSourceImageStreamName(), is.getMetadata().getName());
        assertEquals(namespace, is.getMetadata().getNamespace());
        assertEquals(expectedLabels(kc.getSourceImageStreamName()), is.getMetadata().getLabels());
        assertEquals(false, is.getSpec().getLookupPolicy().getLocal());
        assertEquals(1, is.getSpec().getTags().size());
        assertEquals(image.substring(image.lastIndexOf(":") + 1), is.getSpec().getTags().get(0).getName());
        assertEquals("DockerImage", is.getSpec().getTags().get(0).getFrom().getKind());
        assertEquals(image, is.getSpec().getTags().get(0).getFrom().getName());
        assertNull(is.getSpec().getTags().get(0).getImportPolicy());
        assertNull(is.getSpec().getTags().get(0).getReferencePolicy());
        checkOwnerReference(kc.createOwnerReference(), is);
    }

    @Test
    public void testInsecureSourceRepo() {
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(ResourceUtils.createKafkaConnectS2ICluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout,  metricsCmJson, configurationJson, true, bootstrapServers), VERSIONS);

        assertTrue(kc.isInsecureSourceRepository());

        ImageStream is = kc.generateSourceImageStream();

        assertEquals(kc.getSourceImageStreamName(), is.getMetadata().getName());
        assertEquals(namespace, is.getMetadata().getNamespace());
        assertEquals(expectedLabels(kc.getSourceImageStreamName()), is.getMetadata().getLabels());
        assertEquals(false, is.getSpec().getLookupPolicy().getLocal());
        assertEquals(1, is.getSpec().getTags().size());
        assertEquals(image.substring(image.lastIndexOf(":") + 1), is.getSpec().getTags().get(0).getName());
        assertEquals("DockerImage", is.getSpec().getTags().get(0).getFrom().getKind());
        assertEquals(image, is.getSpec().getTags().get(0).getFrom().getName());
        assertTrue(is.getSpec().getTags().get(0).getImportPolicy().getInsecure());
        assertEquals("Local", is.getSpec().getTags().get(0).getReferencePolicy().getType());
    }

    @Test
    public void testGenerateTargetImageStream() {
        ImageStream is = kc.generateTargetImageStream();

        assertEquals(kc.kafkaConnectClusterName(cluster), is.getMetadata().getName());
        assertEquals(namespace, is.getMetadata().getNamespace());
        assertEquals(expectedLabels(kc.kafkaConnectClusterName(cluster)), is.getMetadata().getLabels());
        assertEquals(true, is.getSpec().getLookupPolicy().getLocal());
        checkOwnerReference(kc.createOwnerReference(), is);
    }

    @Test
    public void withOldAffinity() throws IOException {
        resourceTester
            .assertDesiredResource("-DeploymentConfig.yaml", kcc -> kcc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withAffinity() throws IOException {
        resourceTester
                .assertDesiredResource("-DeploymentConfig.yaml", kcc -> kcc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withOldTolerations() throws IOException {
        resourceTester
                .assertDesiredResource("-DeploymentConfig.yaml", kcc -> kcc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @Test
    public void withTolerations() throws IOException {
        resourceTester
            .assertDesiredResource("-DeploymentConfig.yaml", kcc -> kcc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @Test
    public void testGenerateDeploymentConfigWithTls() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                .endTls()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);

        assertEquals("my-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
        assertEquals("my-another-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertEquals(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS));
        assertEquals("true",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS));
    }

    @Test
    public void testGenerateDeploymentConfigWithTlsAuth() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaConnectAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);

        assertEquals("user-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("user-secret/user.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT));
        assertEquals("user-secret/user.key",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY));
        assertEquals("true",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS));
    }

    @Test
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaConnectAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);

        // 2 = 1 volume from logging/metrics + just 1 from above certs Secret
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getVolumes().size());
        assertEquals("my-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
    }

    @Test
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaConnectAuthenticationScramSha512Builder()
                                .withUsername("user1")
                                .withNewPasswordSecret()
                                .withSecretName("user1-secret")
                                .withPassword("password")
                                .endPasswordSecret()
                                .build()
                )
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);

        assertEquals("user1-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaConnectS2ICluster.PASSWORD_VOLUME_MOUNT + "user1-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());

        assertEquals("user1-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectS2ICluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE));
        assertEquals("user1",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectS2ICluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME));
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

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnots = TestUtils.map("a7", "v7", "a8", "v8");

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
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
                        .withNewApiService()
                            .withNewMetadata()
                                .withLabels(svcLabels)
                                .withAnnotations(svcAnots)
                            .endMetadata()
                        .endApiService()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnots)
                            .endMetadata()
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertTrue(dep.getMetadata().getLabels().entrySet().containsAll(depLabels.entrySet()));
        assertTrue(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()));

        // Check Pods
        assertTrue(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()));
        assertTrue(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()));

        // Check Service
        Service svc = kc.generateService();
        assertTrue(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()));
        assertTrue(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertTrue(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()));
        assertTrue(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()));
    }

    @Test
    public void testExternalConfigurationSecretEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check DeploymentConfig
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertEquals(1, selected.size());
        assertEquals("MY_ENV_VAR", selected.get(0).getName());
        assertEquals(env.getValueFrom().getSecretKeyRef(), selected.get(0).getValueFrom().getSecretKeyRef());
    }

    @Test
    public void testExternalConfigurationConfigEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check DeploymentConfig
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertEquals(1, selected.size());
        assertEquals("MY_ENV_VAR", selected.get(0).getName());
        assertEquals(env.getValueFrom().getConfigMapKeyRef(), selected.get(0).getValueFrom().getConfigMapKeyRef());
    }

    @Test
    public void testExternalConfigurationSecretVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check DeploymentConfig
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(1, selected.size());
        assertEquals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume", selected.get(0).getName());
        assertEquals(volume.getSecret(), selected.get(0).getSecret());

        List<VolumeMount> volumeMounths = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(1, selected.size());
        assertEquals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume", selectedVolumeMounths.get(0).getName());
        assertEquals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume", selectedVolumeMounths.get(0).getMountPath());
    }

    @Test
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check DeploymentConfig
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(1, selected.size());
        assertEquals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume", selected.get(0).getName());
        assertEquals(volume.getConfigMap(), selected.get(0).getConfigMap());

        List<VolumeMount> volumeMounths = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(1, selected.size());
        assertEquals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume", selectedVolumeMounths.get(0).getName());
        assertEquals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume", selectedVolumeMounths.get(0).getMountPath());
    }

    @Test
    public void testExternalConfigurationInvalidVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(0, selected.size());

        List<VolumeMount> volumeMounths = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(0, selected.size());
    }

    @Test
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(0, selected.size());

        List<VolumeMount> volumeMounths = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertEquals(0, selected.size());
    }

    @Test
    public void testInvalidExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertEquals(0, selected.size());
    }

    @Test
    public void testNoExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                .endValueFrom()
                .build();

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertEquals(0, selected.size());
    }

    @Test
    public void testGracePeriod() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testDefaultGracePeriod() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource).build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertEquals(Long.valueOf(30), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(this.resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, secrets);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertFalse(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource).build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertEquals(0, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
    }

    @Test
    public void testSecurityContext() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertNotNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup());
        assertEquals(Long.valueOf(456), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup());
        assertEquals(Long.valueOf(789), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser());
    }

    @Test
    public void testDefaultSecurityContext() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource).build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        assertNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
    }

    @Test
    public void testPodDisruptionBudget() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(2), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource).build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(1), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testImagePullPolicy() {
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, ImagePullPolicy.ALWAYS, null);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());

        dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, ImagePullPolicy.IFNOTPRESENT, null);
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

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(limits, cont.getResources().getLimits());
        assertEquals(requests, cont.getResources().getRequests());
    }

    @Test
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaConnectS2I resource = new KafkaConnectS2IBuilder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withNewXms("512m")
                        .withNewXmx("1024m")
                        .withNewServer(true)
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaConnectS2ICluster kc = KafkaConnectS2ICluster.fromCrd(resource, VERSIONS);

        DeploymentConfig dep = kc.generateDeploymentConfig(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-server"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"));
        assertTrue(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"));
    }
}
