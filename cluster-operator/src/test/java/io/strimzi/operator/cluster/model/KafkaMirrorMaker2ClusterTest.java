/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSourceBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.tracing.OpenTelemetryTracing;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
@ParallelSuite
public class KafkaMirrorMaker2ClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String clusterName = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName, "metrics-config.yml");
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);
    private final String configurationJson = "foo: bar";
    private final String bootstrapServers = "foo-kafka:9092";
    private final String targetClusterAlias = "target";
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;

    private final OrderedProperties defaultConfiguration = new OrderedProperties()
            .addPair("config.storage.topic", "mirrormaker2-cluster-configs")
            .addPair("group.id", "mirrormaker2-cluster")
            .addPair("status.storage.topic", "mirrormaker2-cluster-status")
            .addPair("config.providers.file.class", "org.apache.kafka.common.config.provider.FileConfigProvider")
            .addPair("offset.storage.topic", "mirrormaker2-cluster-offsets")
            .addPair("config.providers", "file")
            .addPair("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .addPair("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .addPair("header.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");

    private final OrderedProperties expectedConfiguration = new OrderedProperties()
            .addMapPairs(defaultConfiguration.asMap())
            .addPair("foo", "bar");

    private final KafkaMirrorMaker2ClusterSpec targetCluster = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(targetClusterAlias)
            .withBootstrapServers(bootstrapServers)
            .withConfig((Map<String, Object>) TestUtils.fromYamlString(configurationJson, Map.class))
            .build();

    private final KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(ResourceUtils.createEmptyKafkaMirrorMaker2(namespace, clusterName))
            .withNewSpec()
                .withImage(image)
                .withReplicas(replicas)
                .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                .withConnectCluster(targetClusterAlias)
                .withClusters(targetCluster)
            .endSpec()
            .build();

    private final KafkaMirrorMaker2 resourceWithMetrics = new KafkaMirrorMaker2Builder(resource)
            .editSpec()
                .withMetricsConfig(jmxMetricsConfig)
            .endSpec()
            .build();

    private final KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resourceWithMetrics, VERSIONS);
    {
        // we were setting metricsEnabled in fromCrd, which was just checking it for non-null. With metrics in CM, we have to check
        // its content, what is done in generateMetricsAndLogConfigMap
        kmm2.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));
    }

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kmm2.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertThat(metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS), is(metricsCmJson));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.clusterName,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaMirrorMaker2Cluster.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaMirrorMaker2Cluster.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, this.clusterName,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.clusterName,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaMirrorMaker2Resources.deploymentName(clusterName));
    }

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION).withValue(expectedConfiguration.asPairs()).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS).withValue(targetClusterAlias).build());
        return expected;
    }

    private Container getContainer(Deployment dep) {
        return dep.getSpec().getTemplate().getSpec().getContainers().get(0);
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createEmptyKafkaMirrorMaker2(namespace, clusterName), VERSIONS);

        assertThat(kmm2.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_CONNECT_IMAGE));
        assertThat(kmm2.replicas, is(KafkaMirrorMaker2Cluster.DEFAULT_REPLICAS));
        assertThat(kmm2.readinessProbeOptions.getInitialDelaySeconds(), is(KafkaMirrorMaker2Cluster.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(kmm2.readinessProbeOptions.getTimeoutSeconds(), is(KafkaMirrorMaker2Cluster.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(kmm2.livenessProbeOptions.getInitialDelaySeconds(), is(KafkaMirrorMaker2Cluster.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(kmm2.livenessProbeOptions.getTimeoutSeconds(), is(KafkaMirrorMaker2Cluster.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(kmm2.getConfiguration().asOrderedProperties(), is(defaultConfiguration));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(kmm2.replicas, is(replicas));
        assertThat(kmm2.image, is(image));
        assertThat(kmm2.readinessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kmm2.readinessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kmm2.livenessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kmm2.livenessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kmm2.getConfiguration().asOrderedProperties(), is(expectedConfiguration));
        assertThat(kmm2.bootstrapServers, is(bootstrapServers));
    }

    @ParallelTest
    public void testEnvVars() {
        assertThat(kmm2.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = kmm2.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kmm2.getComponentName())));
        assertThat(svc.getMetadata().getAnnotations().size(), is(0));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaMirrorMaker2Cluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaMirrorMaker2Cluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics()   {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withMetricsConfig(null)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Service svc = kmm2.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kmm2.getComponentName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaMirrorMaker2Cluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaMirrorMaker2Cluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testGenerateDeployment()   {
        Deployment dep = kmm2.generateDeployment(
                new HashMap<>(), true, null, null);

        assertThat(dep.getMetadata().getName(), is(KafkaMirrorMaker2Resources.deploymentName(clusterName)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        Map<String, String> expectedDeploymentLabels = expectedLabels();
        assertThat(dep.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));
        assertThat(dep.getSpec().getReplicas(), is(replicas));
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().size(), is(1));
        Container cont = getContainer(dep);
        assertThat(cont.getName(), is(KafkaMirrorMaker2Resources.deploymentName(this.clusterName)));
        assertThat(cont.getImage(), is(kmm2.image));
        assertThat(cont.getEnv(), is(getExpectedEnvVars()));
        assertThat(cont.getLivenessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(cont.getLivenessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(cont.getReadinessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(cont.getReadinessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(cont.getPorts().size(), is(2));
        assertThat(cont.getPorts().get(0).getContainerPort(), is(KafkaMirrorMaker2Cluster.REST_API_PORT));
        assertThat(cont.getPorts().get(0).getName(), is(KafkaMirrorMaker2Cluster.REST_API_PORT_NAME));
        assertThat(cont.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal(), is(1));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal(), is(0));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream()
            .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
            .findFirst().get().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

        TestUtils.checkOwnerReference(dep, resource);
    }

    @ParallelTest
    public void withAffinity() throws IOException {
        ResourceTester<KafkaMirrorMaker2, KafkaMirrorMaker2Cluster> resourceTester = new ResourceTester<>(KafkaMirrorMaker2.class, VERSIONS, (kafkaMirrorMaker2, versions) -> KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, versions), this.getClass().getSimpleName() + ".withAffinity");
        resourceTester
            .assertDesiredModel("-Deployment.yaml", kmm2c -> kmm2c.generateDeployment(
                    new HashMap<>(), true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<KafkaMirrorMaker2, KafkaMirrorMaker2Cluster> resourceTester = new ResourceTester<>(KafkaMirrorMaker2.class, VERSIONS, (kafkaMirrorMaker2, versions) -> KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, versions), this.getClass().getSimpleName() + ".withTolerations");
        resourceTester
            .assertDesiredModel("-Deployment.yaml", kmm2c -> kmm2c.generateDeployment(
                    new HashMap<>(), true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @ParallelTest
    public void testGenerateDeploymentWithTls() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTls = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                .endTls()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithTls)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("my-another-secret"));

        Container cont = getContainer(dep);
        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                is("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS), is("true"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS),
                is("target=my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsWithoutCerts() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTls = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewTls()
                .endTls()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithTls)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        Container cont = getContainer(dep);

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                is(nullValue()));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS), is("true"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS),
                is(nullValue()));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithTlsAuth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("user-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT), is("user-secret/user.crt"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY), is("user-secret/user.key"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithTlsAuth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        // 3 = 1 volume from logging/metrics + 2 from above cert mounted for connect and for connectors
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(4));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()  
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithScramSha512Auth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-512"));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithScramSha512AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
            .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
            .endTls()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername("user1")
                .withNewPasswordSecret()
                    .withSecretName("my-secret")
                    .withPassword("user1.password")
                .endPasswordSecret()
            .endKafkaClientAuthenticationScramSha512()
            .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth)
            .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(4));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().size(), is(6));
        assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-512"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication for multiple clusters.
     * It checks that the volumes and volume mounts that reference the secret are correctly created and that each volume name and volume mount path is only
     * created once - duplicate volume names and duplicate volume mount paths will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithMultipleClustersScramSha512AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
            .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
            .endTls()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername("user1")
                .withNewPasswordSecret()
                    .withSecretName("my-secret")
                    .withPassword("user1.password")
                .endPasswordSecret()
            .endKafkaClientAuthenticationScramSha512()
            .build();
        KafkaMirrorMaker2ClusterSpec sourceClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterWithScramSha512Auth)
            .withAlias("source")
            .withBootstrapServers("source-bootstrap-kafka:9092")
            .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth, sourceClusterWithScramSha512Auth)
            .endSpec()
            .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(5));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().size(), is(8));
        assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(6).getName(), is("source-my-secret"));
        assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
        assertThat(cont.getVolumeMounts().get(7).getName(), is("source-my-secret"));
        assertThat(cont.getVolumeMounts().get(7).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "source/my-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-512"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha256Auth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewKafkaClientAuthenticationScramSha256()
                .withUsername("user1")
                .withNewPasswordSecret()
                .withSecretName("user1-secret")
                .withPassword("password")
                .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha256()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithScramSha256Auth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-256"));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 256 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithScramSha256AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationScramSha256()
                .withUsername("user1")
                .withNewPasswordSecret()
                .withSecretName("my-secret")
                .withPassword("user1.password")
                .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha256()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithScramSha256Auth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(4));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().size(), is(6));
        assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-256"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 256 client authentication for multiple clusters.
     * It checks that the volumes and volume mounts that reference the secret are correctly created and that each volume name and volume mount path is only
     * created once - duplicate volume names and duplicate volume mount paths will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithMultipleClustersScramSha256AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationScramSha256()
                .withUsername("user1")
                .withNewPasswordSecret()
                .withSecretName("my-secret")
                .withPassword("user1.password")
                .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha256()
                .build();
        KafkaMirrorMaker2ClusterSpec sourceClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterWithScramSha256Auth)
                .withAlias("source")
                .withBootstrapServers("source-bootstrap-kafka:9092")
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithScramSha256Auth, sourceClusterWithScramSha256Auth)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(5));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().size(), is(8));
        assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(6).getName(), is("source-my-secret"));
        assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
        assertThat(cont.getVolumeMounts().get(7).getName(), is("source-my-secret"));
        assertThat(cont.getVolumeMounts().get(7).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "source/my-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-256"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithPlainAuth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithPlainAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()  
                    .withClusters(targetClusterWithPlainAuth)                
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("plain"));
    }


    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for plain client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithPlainAuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithPlainAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
            .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
            .endTls()
            .withNewKafkaClientAuthenticationPlain()
                .withUsername("user1")
                .withNewPasswordSecret()
                    .withSecretName("my-secret")
                    .withPassword("user1.password")
                .endPasswordSecret()
            .endKafkaClientAuthenticationPlain()
            .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
            .editSpec()
                .withClusters(targetClusterWithPlainAuth)
            .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().toString(), dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(4));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

        Container cont = getContainer(dep);

        assertThat(cont.getVolumeMounts().size(), is(6));
        assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
        assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "plain"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnots = TestUtils.map("a7", "v7", "a8", "v8");

        Map<String, String> crbLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> crbAnots = TestUtils.map("a9", "v9", "a10", "v10");

        Map<String, String> saLabels = TestUtils.map("l11", "v11", "l12", "v12");
        Map<String, String> saAnots = TestUtils.map("a11", "v11", "a12", "v12");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewRack("my-topology-key")
                    .withNewTemplate()
                        .withNewDeployment()
                            .withNewMetadata()
                                .withLabels(depLabels)
                                .withAnnotations(depAnots)
                            .endMetadata()
                            .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                        .endDeployment()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnots)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withHostAliases(hostAlias1, hostAlias2)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                        .endPod()
                        .withNewApiService()
                            .withNewMetadata()
                                .withLabels(svcLabels)
                                .withAnnotations(svcAnots)
                            .endMetadata()
                            .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                            .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                        .endApiService()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnots)
                            .endMetadata()
                        .endPodDisruptionBudget()
                        .withNewClusterRoleBinding()
                            .withNewMetadata()
                                .withLabels(crbLabels)
                                .withAnnotations(crbAnots)
                            .endMetadata()
                        .endClusterRoleBinding()
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate(), is(nullValue()));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream()
            .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
            .findFirst().get().getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));

        // Check Service
        Service svc = kmm2.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check PodDisruptionBudget
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = kmm2.generatePodDisruptionBudgetV1Beta1();
        assertThat(pdbV1Beta1.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdbV1Beta1.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kmm2.generateClusterRoleBinding();
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kmm2.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testExternalConfigurationSecretEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()

                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<EnvVar> envs = getContainer(dep).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
        assertThat(selected.get(0).getValueFrom().getSecretKeyRef(), is(env.getValueFrom().getSecretKeyRef()));
    }

    @ParallelTest
    public void testExternalConfigurationConfigEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<EnvVar> envs = getContainer(dep).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
        assertThat(selected.get(0).getValueFrom().getConfigMapKeyRef(), is(env.getValueFrom().getConfigMapKeyRef()));
    }

    @ParallelTest
    public void testExternalConfigurationSecretVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

        List<VolumeMount> volumeMounths = getContainer(dep).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selectedVolumeMounths.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selectedVolumeMounths.get(0).getMountPath(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
    }

    @ParallelTest
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

        List<VolumeMount> volumeMounths = getContainer(dep).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selectedVolumeMounths.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selectedVolumeMounths.get(0).getMountPath(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
    }

    @ParallelTest
    public void testExternalConfigurationInvalidVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));

        List<VolumeMount> volumeMounths = getContainer(dep).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));

        List<VolumeMount> volumeMounths = getContainer(dep).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testInvalidExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<EnvVar> envs = getContainer(dep).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testNoExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        List<EnvVar> envs = getContainer(dep).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(123L));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(30L));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        kmm2.securityProvider = new RestrictedPodSecurityProvider();
        kmm2.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        Deployment dep = kmm2.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));

        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = kmm2.generatePodDisruptionBudgetV1Beta1();
        assertThat(pdbV1Beta1.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));

        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = kmm2.generatePodDisruptionBudgetV1Beta1();
        assertThat(pdbV1Beta1.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                Collections.EMPTY_MAP, true, ImagePullPolicy.ALWAYS, null);
        assertThat(getContainer(dep).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = kmm2.generateDeployment(
                Collections.EMPTY_MAP, true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(getContainer(dep).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                Collections.EMPTY_MAP, true, null, null);
        Container cont = getContainer(dep);
        assertThat(cont.getResources().getLimits(), is(limits));
        assertThat(cont.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withXms("512m")
                        .withXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                Collections.EMPTY_MAP, true, null, null);
        Container cont = getContainer(dep);
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
    }

    @ParallelTest
    public void testKafkaMirrorMaker2ContainerEnvVars() {

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
        ContainerTemplate kafkaMirrorMaker2Container = new ContainerTemplate();
        kafkaMirrorMaker2Container.setEnv(testEnvs);

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaMirrorMaker2Container)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testKafkaContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaMirrorMaker2Container = new ContainerTemplate();
        kafkaMirrorMaker2Container.setEnv(testEnvs);

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaMirrorMaker2Container)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testJaegerTracing() {
        testTracing(JaegerTracing.TYPE_JAEGER,
                JaegerTracing.CONSUMER_INTERCEPTOR_CLASS_NAME,
                JaegerTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
    }

    @ParallelTest
    public void testOpenTelemetryTracing() {
        testTracing(OpenTelemetryTracing.TYPE_OPENTELEMETRY,
                OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME,
                OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
    }

    public void testTracing(String type, String consumerInterceptor, String producerInterceptor) {
        KafkaMirrorMaker2Builder builder = new KafkaMirrorMaker2Builder(this.resource);
        switch (type) {
            case JaegerTracing.TYPE_JAEGER:
                builder.editSpec()
                            .withNewJaegerTracing()
                            .endJaegerTracing()
                        .endSpec();
                break;
            case OpenTelemetryTracing.TYPE_OPENTELEMETRY:
                builder.editSpec()
                            .withNewOpenTelemetryTracing()
                            .endOpenTelemetryTracing()
                        .endSpec();
                break;
            default:
                throw new IllegalArgumentException("The '" + type + "' is not a valid tracing type");
        }
        KafkaMirrorMaker2 resource = builder.build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kmm2.generateDeployment(
                Collections.EMPTY_MAP, true, null, null);
        Container cont = getContainer(dep);
        assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMaker2Cluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals(type), is(true));
        assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("consumer.interceptor.classes=" + consumerInterceptor), is(true));
        assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("producer.interceptor.classes=" + producerInterceptor), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithAccessToken() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithAccessToken = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                    new KafkaClientAuthenticationOAuthBuilder()
                            .withNewAccessToken()
                                .withSecretName("my-token-secret")
                                .withKey("my-token-key")
                            .endAccessToken()
                            .build())  
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithAccessToken)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        Container cont = getContainer(dep);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithRefreshToken() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithRefreshToken = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                    new KafkaClientAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withTokenEndpointUri("http://my-oauth-server")
                            .withConnectTimeoutSeconds(15)
                            .withReadTimeoutSeconds(15)
                            .withEnableMetrics(true)
                            .withNewRefreshToken()
                                .withSecretName("my-token-secret")
                                .withKey("my-token-key")
                            .endRefreshToken()
                            .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithRefreshToken)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        Container cont = getContainer(dep);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                        ClientConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_READ_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_ENABLE_METRICS, "true")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithClientSecret = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                    new KafkaClientAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withTokenEndpointUri("http://my-oauth-server")
                            .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                            .endClientSecret()
                            .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithClientSecret)                
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        Container cont = getContainer(dep);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithUsernameAndPassword() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithUsernameAndPassword = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withUsername("user1")
                                .withNewPasswordSecret()
                                    .withSecretName("my-password-secret")
                                    .withPassword("user1.password")
                                .endPasswordSecret()
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithOAuthWithUsernameAndPassword)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        Container cont = getContainer(dep);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-password-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("user1.password"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_PASSWORD_GRANT_USERNAME, "user1", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithMissingClientSecret = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://my-oauth-server")
                                    .build())
                    .build();
            KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                    .editSpec()
                        .withClusters(targetClusterWithOAuthWithMissingClientSecret)  
                    .endSpec()
                    .build();

            KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithMissingUri = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                    .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                    .build();
            KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                    .editSpec()
                        .withClusters(targetClusterWithOAuthWithMissingUri)                      
                    .endSpec()
                    .build();

            KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
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

        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithTls = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithTls)                
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kmm2.generateDeployment(
                emptyMap(), true, null, null);
        Container cont = getContainer(dep);

        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/first-certificate-2"));

        // Volumes
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca2.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));
    }

    @ParallelTest
    public void testNetworkPolicy() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resourceWithMetrics)
                .build();
        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        kc.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", null);

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));

        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Map.of()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperatorSameNamespace() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resourceWithMetrics)
                .build();
        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        kc.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));

        NetworkPolicy np = kc.generateNetworkPolicy(true, namespace, null);

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperatorWithNamespaceLabels() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resourceWithMetrics)
                .build();
        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        kc.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.METRICS_PORT));
    }


    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS);

        assertThat(kmm.isMetricsEnabled(), is(true));
        assertThat(kmm.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testJmxSecretCustomLabelsAndAnnotations() {
        Map<String, String> customLabels = new HashMap<>(2);
        customLabels.put("label1", "value1");
        customLabels.put("label2", "value2");

        Map<String, String> customAnnotations = new HashMap<>(2);
        customAnnotations.put("anno1", "value3");
        customAnnotations.put("anno2", "value4");


        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                    .build())
                            .build())
                    .withNewTemplate()
                        .withNewJmxSecret()
                            .withNewMetadata()
                                .withAnnotations(customAnnotations)
                                .withLabels(customLabels)
                            .endMetadata()
                        .endJmxSecret()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS);

        Secret jmxSecret = kmm2.generateJmxSecret();

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        KafkaMirrorMaker2Cluster kmm = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        assertThat(kmm.isMetricsEnabled(), is(false));
        assertThat(kmm.getMetricsConfigInCm(), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateDeploymentWithRack() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editOrNewSpec()
                    .withNewRack()
                        .withTopologyKey("topology-key")
                    .endRack()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment deployment = cluster.generateDeployment(new HashMap<>(), false, null, null);

        if (resource.getSpec().getRack() != null) {
            PodSpec podSpec = deployment.getSpec().getTemplate().getSpec();

            // check that pod spec contains the init Kafka container
            List<Container> initContainers = podSpec.getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(initContainers.size() > 0, is(true));

            boolean isKafkaInitContainer =
                    initContainers.stream().anyMatch(container -> container.getName().equals(KafkaConnectCluster.INIT_NAME));
            assertThat(isKafkaInitContainer, is(true));
        }
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";
        String topologyKey = "topology-key";

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editOrNewSpec()
                    .withNewRack(topologyKey)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(clusterName, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(cluster.componentName));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .build();

        KafkaConnectCluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb, is(nullValue()));
    }
}
