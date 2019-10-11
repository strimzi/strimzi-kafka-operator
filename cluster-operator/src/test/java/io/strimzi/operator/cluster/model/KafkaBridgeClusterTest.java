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
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaBridgeClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
    private final String image = "my-image:latest";
    private final int healthDelay = 15;
    private final int healthTimeout = 5;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String bootstrapServers = "foo-kafka:9092";
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;
    private final String defaultProducerConfiguration = "";
    private final String defaultConsumerConfiguration = "";

    private final KafkaBridge resource = new KafkaBridgeBuilder(ResourceUtils.createEmptyKafkaBridgeCluster(namespace, cluster))
            .withNewSpec()
            .withMetrics((Map<String, Object>) TestUtils.fromJson(metricsCmJson, Map.class))
            .withImage(image)
            .withReplicas(replicas)
            .withBootstrapServers(bootstrapServers)
            .withNewHttp(8080)
            .endSpec()
            .build();
    private final KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

    @Rule
    public ResourceTester<KafkaBridge, KafkaBridgeCluster> resourceTester = new ResourceTester<>(KafkaBridge.class, VERSIONS, KafkaBridgeCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kbc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaBridgeResources.deploymentName(cluster));
    }

    protected List<EnvVar> getExpectedEnvVars() {

        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG).withValue(defaultConsumerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_PRODUCER_CONFIG).withValue(defaultProducerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_ID).withValue(cluster).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_HTTP_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_HTTP_HOST).withValue(KafkaBridgeHttpConfig.HTTP_DEFAULT_HOST).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_HTTP_PORT).withValue(String.valueOf(KafkaBridgeHttpConfig.HTTP_DEFAULT_PORT)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_AMQP_ENABLED).withValue(String.valueOf(false)).build());
        return expected;
    }

    @Test
    public void testDefaultValues() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(ResourceUtils.createEmptyKafkaBridgeCluster(namespace, cluster), VERSIONS);

        assertEquals("strimzi/kafka-bridge:latest", kbc.image);
        assertEquals(KafkaBridgeCluster.DEFAULT_REPLICAS, kbc.replicas);
        assertEquals(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_DELAY, kbc.readinessProbeOptions.getInitialDelaySeconds());
        assertEquals(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_TIMEOUT, kbc.readinessProbeOptions.getTimeoutSeconds());
        assertEquals(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_DELAY, kbc.livenessProbeOptions.getInitialDelaySeconds());
        assertEquals(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_TIMEOUT, kbc.livenessProbeOptions.getTimeoutSeconds());
    }

    @Test
    public void testFromCrd() {
        assertEquals(replicas, kbc.replicas);
        assertEquals(image, kbc.image);
        assertEquals(healthDelay, kbc.readinessProbeOptions.getInitialDelaySeconds());
        assertEquals(healthTimeout, kbc.readinessProbeOptions.getTimeoutSeconds());
        assertEquals(healthDelay, kbc.livenessProbeOptions.getInitialDelaySeconds());
        assertEquals(healthTimeout, kbc.livenessProbeOptions.getTimeoutSeconds());
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), kbc.getEnvVars());
    }

    @Test
    public void testGenerateService()   {
        Service svc = kbc.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        assertEquals(expectedLabels(kbc.getServiceName()), svc.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), svc.getSpec().getSelector());
        assertEquals(2, svc.getSpec().getPorts().size());
        assertEquals(new Integer(KafkaBridgeCluster.DEFAULT_REST_API_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaBridgeCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
        assertEquals(emptyMap(), svc.getMetadata().getAnnotations());
        checkOwnerReference(kbc.createOwnerReference(), svc);
    }

    @Test
    public void testGenerateDeployment()   {
        Deployment dep = kbc.generateDeployment(new HashMap<String, String>(), true, null, null);

        assertEquals(KafkaBridgeResources.deploymentName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = expectedLabels();
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), dep.getSpec().getSelector().getMatchLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(KafkaBridgeResources.deploymentName(cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(kbc.image, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        assertEquals(new Integer(KafkaBridgeCluster.DEFAULT_REST_API_PORT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort());
        assertEquals(KafkaBridgeCluster.REST_API_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());
        assertEquals("RollingUpdate", dep.getSpec().getStrategy().getType());
        assertEquals(new Integer(1), dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal());
        assertEquals(new Integer(0), dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal());
        assertNull(AbstractModel.containerEnvVars(dep.getSpec().getTemplate().getSpec().getContainers().get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS));
        checkOwnerReference(kbc.createOwnerReference(), dep);
    }

    @Test
    public void testGenerateDeploymentWithTls() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                    .endTls()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("my-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
        assertEquals("my-another-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertEquals(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS));
        assertEquals("true",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS));
    }

    @Test
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                    .endTls()
                    .withAuthentication(
                            new KafkaClientAuthenticationTlsBuilder()
                                    .withNewCertificateAndKey()
                                    .withSecretName("user-secret")
                                    .withCertificate("user.crt")
                                    .withKey("user.key")
                                    .endCertificateAndKey()
                                    .build())
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("user-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret",
                containers.get(0).getVolumeMounts().get(2).getMountPath());

        assertEquals("user-secret/user.crt",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT));
        assertEquals("user-secret/user.key",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY));
        assertEquals("true",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS));
    }

    @Test
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                    .endTls()
                    .withAuthentication(
                            new KafkaClientAuthenticationTlsBuilder()
                                    .withNewCertificateAndKey()
                                    .withSecretName("my-secret")
                                    .withCertificate("user.crt")
                                    .withKey("user.key")
                                    .endCertificateAndKey()
                                    .build())
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        // 2 = 1 volume from logging/metrics + just 1 from above certs Secret
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getVolumes().size());
        assertEquals("my-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());
    }

    @Test
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("user1-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());

        assertEquals("user1-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE));
        assertEquals("user1",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME));
        assertEquals("scram-sha-512", 
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM));
    }

    @Test
    public void testGenerateDeploymentWithPlainAuth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewKafkaClientAuthenticationPlain()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertEquals("user1-secret", dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName());

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret",
                containers.get(0).getVolumeMounts().get(1).getMountPath());

        assertEquals("user1-secret/password",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE));
        assertEquals("user1",
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME));
        assertEquals("plain", 
                AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM));
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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        // Check Deployment
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertTrue(dep.getMetadata().getLabels().entrySet().containsAll(depLabels.entrySet()));
        assertTrue(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()));
        assertEquals("top-priority", dep.getSpec().getTemplate().getSpec().getPriorityClassName());

        // Check Pods
        assertTrue(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()));
        assertTrue(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()));

        // Check Service
        Service svc = kbc.generateService();
        assertTrue(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()));
        assertTrue(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertTrue(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()));
        assertTrue(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()));
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertEquals(1, resource.getMetadata().getOwnerReferences().size());
        assertEquals(ownerRef, resource.getMetadata().getOwnerReferences().get(0));
    }

    @Test
    public void testGracePeriod() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testDefaultGracePeriod() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(Long.valueOf(30), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
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

        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(this.resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, secrets);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertFalse(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertEquals(0, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
    }

    @Test
    public void testSecurityContext() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertNotNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup());
        assertEquals(Long.valueOf(456), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup());
        assertEquals(Long.valueOf(789), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser());
    }

    @Test
    public void testDefaultSecurityContext() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
    }

    @Test
    public void testPodDisruptionBudget() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(2), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(1), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testImagePullPolicy() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.ALWAYS, null);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());

        dep = kbc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.IFNOTPRESENT, null);
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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(limits, cont.getResources().getLimits());
        assertEquals(requests, cont.getResources().getRequests());
    }

    @Test
    public void testKafkaBridgeContainerEnvVars() {

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
        ContainerTemplate kafkaBridgeContainer = new ContainerTemplate();
        kafkaBridgeContainer.setEnv(testEnvs);

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withBridgeContainer(kafkaBridgeContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaBridgeCluster.fromCrd(resource, VERSIONS).getEnvVars();

        assertTrue("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue));
        assertTrue("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue));
    }

    @Test
    public void testKafkaBridgeContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaBridgeContainer = new ContainerTemplate();
        kafkaBridgeContainer.setEnv(testEnvs);

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withBridgeContainer(kafkaBridgeContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaBridgeCluster.fromCrd(resource, VERSIONS).getEnvVars();

        assertFalse("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue));
        assertFalse("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue));
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithAccessToken() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withNewAccessToken()
                                    .withSecretName("my-token-secret")
                                    .withKey("my-token-key")
                                .endAccessToken()
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-token-secret", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-token-key", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertTrue(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElse(null).getValue().isEmpty());
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithRefreshToken() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewRefreshToken()
                                    .withSecretName("my-token-secret")
                                    .withKey("my-token-key")
                                .endRefreshToken()
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-token-secret", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-token-key", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server"), cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElse(null).getValue().trim());
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server"), cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElse(null).getValue().trim());
    }

    @Test(expected = InvalidResourceException.class)
    public void testGenerateDeploymentWithOAuthWithMissingClientSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster.fromCrd(resource, VERSIONS);
    }

    @Test(expected = InvalidResourceException.class)
    public void testGenerateDeploymentWithOAuthWithMissingUri() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster.fromCrd(resource, VERSIONS);
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithTls() {
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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
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
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals("oauth", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElse(null).getValue());
        assertEquals("my-secret-secret", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName());
        assertEquals("my-secret-key", cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey());
        assertEquals(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""), cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElse(null).getValue().trim());

        // Volume mounts
        assertEquals(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath());
        assertEquals(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath());

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
    public void testDifferentHttpPort()   {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewHttp(1874)
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(resource, VERSIONS);

        // Check ports in container
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals(new IntOrString(KafkaBridgeCluster.REST_API_PORT_NAME), cont.getLivenessProbe().getHttpGet().getPort());
        assertEquals(new IntOrString(KafkaBridgeCluster.REST_API_PORT_NAME), cont.getReadinessProbe().getHttpGet().getPort());
        assertEquals(new Integer(1874), cont.getPorts().get(0).getContainerPort());
        assertEquals(KafkaBridgeCluster.REST_API_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());

        // Check ports on Service
        Service svc = kb.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        assertEquals(expectedLabels(kb.getServiceName()), svc.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), svc.getSpec().getSelector());
        assertEquals(new Integer(1874), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaBridgeCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
        assertEquals(emptyMap(), svc.getMetadata().getAnnotations());
        checkOwnerReference(kbc.createOwnerReference(), svc);
    }

    @Test
    public void testProbeConfiguration()   {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(20)
                        .withPeriodSeconds(21)
                        .withTimeoutSeconds(22)
                    .endLivenessProbe()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(30)
                        .withPeriodSeconds(31)
                        .withTimeoutSeconds(32)
                    .endReadinessProbe()
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(resource, VERSIONS);
        Deployment dep = kb.generateDeployment(new HashMap<String, String>(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals(new Integer(20), cont.getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(21), cont.getLivenessProbe().getPeriodSeconds());
        assertEquals(new Integer(22), cont.getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(30), cont.getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(31), cont.getReadinessProbe().getPeriodSeconds());
        assertEquals(new Integer(32), cont.getReadinessProbe().getTimeoutSeconds());
    }
}
