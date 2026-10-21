/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.sova.SovaResources;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.MtlsAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneEncryptionConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:NoFullyQualifiedClassNames"}) // Fully qualified class names are used in the expected config strings
public class SovaTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .build())
                .endKafka()
                .withNewExtraMetrics()
                    .withImage("my-image:latest")
                    .withGroupRegex("my-group-.*")
                    .withTopicRegex("my-topic-.*")
                    .withGroupExcludeRegex("my-group-exclude-.*")
                    .withTopicExcludeRegex("my-topic-exclude-.*")
                    .withNewTemplate()
                        .withNewPod()
                            .withTmpDirSizeLimit("100Mi")
                        .endPod()
                    .endTemplate()
                .endExtraMetrics()
            .endSpec()
            .build();

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @Test
    public void testFromCrdReturnsNullWhenNotConfigured() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withExtraMetrics(null)
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertThat(sova, is(nullValue()));
    }

    @Test
    public void testFromCrdWithCustomImage() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertThat(sova, is(notNullValue()));
        assertThat(sova.getImage(), is("my-image:latest"));
    }

    @Test
    public void testFromCrdWithDefaultImage() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withImage(null)
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertThat(sova.getImage(), is(VERSIONS.kafkaImage(null, VERSIONS.defaultVersion().version())));
    }

    @Test
    public void testHeapOptions() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("1Gi"))).build())
                        .withNewJvmOptions()
                            .withJavaSystemProperties(new SystemPropertyBuilder().withName("my.prop").withValue("value").build())
                        .endJvmOptions()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<EnvVar> envVars = sova.getEnvVars();
        assertEnvVar(envVars, "STRIMZI_DYNAMIC_HEAP_PERCENTAGE", "75");
        assertNoEnvVar(envVars, "STRIMZI_DYNAMIC_HEAP_MAX");
        assertEnvVar(envVars, "STRIMZI_JAVA_SYSTEM_PROPERTIES", "-Dmy.prop=value");
    }

    @Test
    public void testDefaultEnvVars() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewExtraMetrics()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<EnvVar> envVars = sova.getEnvVars();
        assertEnvVar(envVars, Sova.ENV_VAR_GROUP_REGEX, ".*");
        assertEnvVar(envVars, Sova.ENV_VAR_TOPIC_REGEX, ".*");
        assertEnvVar(envVars, Sova.ENV_VAR_METRICS_PORT, "9404");
        assertEnvVar(envVars, Sova.ENV_VAR_MANAGEMENT_PORT, "8080");
        assertEnvVar(envVars, Sova.ENV_VAR_KAFKA_ADMIN_CLIENT_CONFIGURATION, sova.kafkaAdminClientConfiguration());
        // Exclude regex vars must not be present when not configured
        assertNoEnvVar(envVars, Sova.ENV_VAR_GROUP_EXCLUDE_REGEX);
        assertNoEnvVar(envVars, Sova.ENV_VAR_TOPIC_EXCLUDE_REGEX);
    }

    @Test
    public void testRegexEnvVars() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<EnvVar> envVars = sova.getEnvVars();
        assertEnvVar(envVars, Sova.ENV_VAR_GROUP_REGEX, "my-group-.*");
        assertEnvVar(envVars, Sova.ENV_VAR_GROUP_EXCLUDE_REGEX, "my-group-exclude-.*");
        assertEnvVar(envVars, Sova.ENV_VAR_TOPIC_REGEX, "my-topic-.*");
        assertEnvVar(envVars, Sova.ENV_VAR_TOPIC_EXCLUDE_REGEX, "my-topic-exclude-.*");
    }

    @Test
    public void testAdminClientConfigurationWithMtls() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        assertThat(sova.kafkaAdminClientConfiguration(), is(String.join("\n",
                "bootstrap.servers=" + KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT,
                "security.protocol=SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.certificates=" + placeholder(KafkaResources.trustBundleSecretName(CLUSTER_NAME), "cluster-ca.crt"),
                "ssl.keystore.type=PEM",
                "ssl.keystore.certificate.chain=" + placeholder(SovaResources.secretName(CLUSTER_NAME), "sova.crt"),
                "ssl.keystore.key=" + placeholder(SovaResources.secretName(CLUSTER_NAME), "sova.key"),
                "config.providers=strimzisecrets",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "")));
    }

    @Test
    public void testAdminClientConfigurationWithServiceAccountAuth() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), serviceAccountAuthentication());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        assertThat(sova.kafkaAdminClientConfiguration(), is(String.join("\n",
                "bootstrap.servers=" + KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT,
                "security.protocol=SASL_SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.certificates=" + placeholder(KafkaResources.trustBundleSecretName(CLUSTER_NAME), "cluster-ca.crt"),
                "sasl.mechanism=OAUTHBEARER",
                "sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location=\"/var/run/secrets/strimzi.io/token\";",
                "config.providers=strimzisecrets",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "")));
    }

    @Test
    public void testAdminClientConfigurationWithTlsOnlyNoAuth() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), new NoneAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        assertThat(sova.kafkaAdminClientConfiguration(), is(String.join("\n",
                "bootstrap.servers=" + KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT,
                "security.protocol=SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.certificates=" + placeholder(KafkaResources.trustBundleSecretName(CLUSTER_NAME), "cluster-ca.crt"),
                "config.providers=strimzisecrets",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "")));
    }

    @Test
    public void testAdminClientConfigurationWithNoTls() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        assertThat(sova.kafkaAdminClientConfiguration(), is(String.join("\n",
                "bootstrap.servers=" + KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT,
                "security.protocol=PLAINTEXT",
                "")));
    }

    @Test
    public void testContainerPorts() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);
        Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getPorts().stream().anyMatch(p -> p.getContainerPort() == MetricsModel.METRICS_PORT), is(true));
        assertThat(container.getPorts().stream().anyMatch(p -> p.getContainerPort() == Sova.MANAGEMENT_PORT), is(true));
    }

    @Test
    public void testProbesUseManagementPort() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);
        Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getLivenessProbe().getHttpGet().getPath(), is("/healthy"));
        assertThat(container.getLivenessProbe().getHttpGet().getPort(), is(new IntOrString(Sova.MANAGEMENT_PORT_NAME)));
        assertThat(container.getReadinessProbe().getHttpGet().getPath(), is("/ready"));
        assertThat(container.getReadinessProbe().getHttpGet().getPort(), is(new IntOrString(Sova.MANAGEMENT_PORT_NAME)));
    }

    @Test
    public void testVolumesWithMtls() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);

        // Certificates are loaded through the config provider, so only the Service Account token is mounted
        List<String> volumeNames = dep.getSpec().getTemplate().getSpec().getVolumes().stream().map(Volume::getName).toList();
        assertThat(volumeNames, containsInAnyOrder(
                VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME,
                VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME,
                Sova.LOG_AND_METRICS_CONFIG_VOLUME_NAME));

        List<String> mountNames = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
                .stream().map(VolumeMount::getName).toList();
        assertThat(mountNames, containsInAnyOrder(
                VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME,
                VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME,
                Sova.LOG_AND_METRICS_CONFIG_VOLUME_NAME));
    }

    @Test
    public void testVolumesWithServiceAccountAuth() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), serviceAccountAuthentication());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);

        List<String> volumeNames = dep.getSpec().getTemplate().getSpec().getVolumes().stream().map(Volume::getName).toList();
        assertThat(volumeNames, containsInAnyOrder(
                VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME,
                VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME,
                VolumeUtils.STRIMZI_AUTHENTICATION_TOKEN_VOLUME_NAME,
                Sova.LOG_AND_METRICS_CONFIG_VOLUME_NAME));

        List<String> mountNames = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
                .stream().map(VolumeMount::getName).toList();
        assertThat(mountNames, containsInAnyOrder(
                VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME,
                VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME,
                VolumeUtils.STRIMZI_AUTHENTICATION_TOKEN_VOLUME_NAME,
                Sova.LOG_AND_METRICS_CONFIG_VOLUME_NAME));
    }

    @Test
    public void testVolumesWithNoTls() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);

        List<String> volumeNames = dep.getSpec().getTemplate().getSpec().getVolumes().stream().map(Volume::getName).toList();
        assertThat(volumeNames, containsInAnyOrder(
                VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME,
                Sova.LOG_AND_METRICS_CONFIG_VOLUME_NAME));
    }

    @Test
    public void testRoleAndRoleBindingWithMtls() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        Role role = sova.generateRole();
        assertThat(role.getMetadata().getName(), is(SovaResources.componentName(CLUSTER_NAME)));
        assertThat(role.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(role.getRules().size(), is(1));
        assertThat(role.getRules().get(0).getApiGroups(), contains(""));
        assertThat(role.getRules().get(0).getResources(), contains("secrets"));
        assertThat(role.getRules().get(0).getVerbs(), contains("get"));
        assertThat(role.getRules().get(0).getResourceNames(), containsInAnyOrder(
                KafkaResources.trustBundleSecretName(CLUSTER_NAME),
                SovaResources.secretName(CLUSTER_NAME)));

        RoleBinding roleBinding = sova.generateRoleBindingForRole();
        assertThat(roleBinding.getMetadata().getName(), is(SovaResources.roleBindingName(CLUSTER_NAME)));
        assertThat(roleBinding.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(roleBinding.getRoleRef().getKind(), is("Role"));
        assertThat(roleBinding.getRoleRef().getName(), is(SovaResources.componentName(CLUSTER_NAME)));
        assertThat(roleBinding.getSubjects().size(), is(1));
        assertThat(roleBinding.getSubjects().get(0).getKind(), is("ServiceAccount"));
        assertThat(roleBinding.getSubjects().get(0).getName(), is(SovaResources.componentName(CLUSTER_NAME)));
        assertThat(roleBinding.getSubjects().get(0).getNamespace(), is(NAMESPACE));
    }

    @Test
    public void testRoleWithServiceAccountAuthAllowsOnlyTrustBundle() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), serviceAccountAuthentication());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        assertThat(sova.generateRole().getRules().get(0).getResourceNames(),
                contains(KafkaResources.trustBundleSecretName(CLUSTER_NAME)));
    }

    @Test
    public void testNoRoleAndRoleBindingWithoutTls() {
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        assertThat(sova.generateRole(), is(nullValue()));
        assertThat(sova.generateRoleBindingForRole(), is(nullValue()));
    }

    @Test
    public void testGenerateDeployment() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);

        assertThat(dep, is(notNullValue()));
        assertThat(dep.getMetadata().getName(), is(SovaResources.componentName(CLUSTER_NAME)));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
    }

    @Test
    public void testGenerateNetworkPolicy() {
        // Sova always produces exactly one ingress rule (port 9404).
        // Kubelet probes hit port 8080 directly and do not require an ingress rule.
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        NetworkPolicy np = sova.generateNetworkPolicy();

        assertThat(np, is(notNullValue()));
        assertThat(np.getMetadata().getName(), is(SovaResources.componentName(CLUSTER_NAME)));
        assertThat(np.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(np.getSpec().getIngress().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
        // Management port (8080) must not have an ingress rule — kubelet probes bypass NetworkPolicy
        assertThat(np.getSpec().getIngress().stream()
                .flatMap(r -> r.getPorts().stream())
                .noneMatch(p -> p.getPort().getIntVal() == Sova.MANAGEMENT_PORT), is(true));
    }

    // Utility methods

    private static String placeholder(String secretName, String key) {
        return "${strimzisecrets:" + NAMESPACE + "/" + secretName + ":" + key + "}";
    }

    private static AuthenticationConfiguration serviceAccountAuthentication() {
        return AuthenticationConfiguration.fromCrd(NAMESPACE, CLUSTER_NAME,
                new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build(), null);
    }

    private static void assertEnvVar(List<EnvVar> envVars, String name, String expectedValue) {
        assertThat("Expected env var " + name,
                envVars.stream().filter(v -> name.equals(v.getName())).map(EnvVar::getValue).findFirst().orElse(null),
                is(expectedValue));
    }

    private static void assertNoEnvVar(List<EnvVar> envVars, String name) {
        assertThat("Unexpected env var " + name,
                envVars.stream().noneMatch(v -> name.equals(v.getName())), is(true));
    }
}
