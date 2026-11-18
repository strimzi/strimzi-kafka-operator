/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.common.template.StrimziDeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.sova.SovaResources;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.MtlsAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneEncryptionConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:NoFullyQualifiedClassNames"}) // Fully qualified class names are used in the expected config strings
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
    public void testGenerateDeploymentWithTmpDirSizeLimit() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        Deployment dep = sova.generateDeployment(Map.of(), false, null, null);

        assertThat(getVolume(dep.getSpec().getTemplate().getSpec(), VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME).getEmptyDir().getSizeLimit(), is(new Quantity("100Mi")));
    }

    @Test
    public void testImagePullPolicy() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        Deployment dep = sova.generateDeployment(Map.of(), true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = sova.generateDeployment(Map.of(), true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @Test
    public void testContainerTemplateEnvVars() {
        ContainerEnvVar envVar1 = new ContainerEnvVarBuilder().withName("TEST_ENV_1").withValue("test.env.one").build();
        ContainerEnvVar envVar2 = new ContainerEnvVarBuilder().withName("TEST_ENV_2").withValue("test.env.two").build();

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .editTemplate()
                            .withNewContainer()
                                .withEnv(envVar1, envVar2)
                            .endContainer()
                        .endTemplate()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<EnvVar> envVars = sova.getEnvVars();
        assertThat(envVars.stream().filter(v -> "TEST_ENV_1".equals(v.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is("test.env.one"));
        assertThat(envVars.stream().filter(v -> "TEST_ENV_2".equals(v.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is("test.env.two"));
    }

    @Test
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVarBuilder().withName("TEST_ENV_1").withValue("test.env.one").build();
        ContainerEnvVar envVar2 = new ContainerEnvVarBuilder().withName(Sova.ENV_VAR_GROUP_REGEX).withValue("my-special-value").build();

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .editTemplate()
                            .withNewContainer()
                                .withEnv(envVar1, envVar2)
                            .endContainer()
                        .endTemplate()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<EnvVar> envVars = sova.getEnvVars();
        assertThat(envVars.stream().filter(v -> "TEST_ENV_1".equals(v.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is("test.env.one"));
        assertThat(envVars.stream().filter(v -> Sova.ENV_VAR_GROUP_REGEX.equals(v.getName())).map(EnvVar::getValue).toList(), contains("my-group-.*"));
    }

    @Test
    public void testTemplate() {
        Map<String, String> depLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnots = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnots = Map.of("a7", "v7", "a8", "v8");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key1")
                                    .withOperator("In")
                                    .withValues("value1", "value2")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> tolerations = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("configMap1")
                .build();

        AdditionalVolume additionalVolumeConfigMap = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();

        VolumeMount additionalVolumeMountConfigMap = new VolumeMountBuilder()
                .withName("config-map-volume-name-2")
                .withMountPath("/mnt/config-map-path")
                .withSubPath("def")
                .build();

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
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
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                                .withTopologySpreadConstraints(tsc1, tsc2)
                                .withEnableServiceLinks(false)
                                .withVolumes(additionalVolumeConfigMap)
                                .withHostUsers(false)
                            .endPod()
                            .withNewContainer()
                                .withVolumeMounts(additionalVolumeMountConfigMap)
                            .endContainer()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnots)
                                .endMetadata()
                            .endServiceAccount()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnots)
                                .endMetadata()
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Check Deployment
        Deployment dep = sova.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostUsers(), is(false));
        assertThat(getVolume(dep.getSpec().getTemplate().getSpec(), additionalVolumeConfigMap.getName()).getConfigMap(), is(configMap));
        assertThat(getVolumeMount(dep.getSpec().getTemplate().getSpec().getContainers().get(0), additionalVolumeMountConfigMap.getName()), is(additionalVolumeMountConfigMap));

        // Check Service Account
        ServiceAccount sa = sova.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));

        // Check Pod Disruption Budget
        PodDisruptionBudget pdb = sova.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @Test
    public void testGenerateDeploymentWithRecreateDeploymentStrategy() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withNewTemplate()
                            .withNewDeployment()
                                .withDeploymentStrategy(StrimziDeploymentStrategy.RECREATE)
                            .endDeployment()
                        .endTemplate()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        Deployment dep = sova.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
    }

    @Test
    public void testSecurityProvider() {
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        sova.securityProvider = new TestPodSecurityProvider();
        sova.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        Deployment dep = sova.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostUsers(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
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

    @Test
    public void testNoTuningSettingsWhenFieldsUnset() {
        Sova withoutConfig = Sova.fromCrd(
                new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        Kafka emptyConfig = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withConfig(Map.of())
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova withEmptyConfig = Sova.fromCrd(
                new Reconciliation("test", emptyConfig.getKind(), NAMESPACE, CLUSTER_NAME),
                emptyConfig, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertNoEnvVar(withoutConfig.getEnvVars(), Sova.ENV_VAR_COLLECT_INTERVAL_MS);
        assertThat(withoutConfig.kafkaAdminClientConfiguration(), not(containsString("default.api.timeout.ms")));
        assertThat(withEmptyConfig.kafkaAdminClientConfiguration(), is(withoutConfig.kafkaAdminClientConfiguration()));
    }

    @Test
    public void testCollectionIntervalEnvVar() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withCollectionIntervalMs(5000L)
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        assertEnvVar(sova.getEnvVars(), Sova.ENV_VAR_COLLECT_INTERVAL_MS, "5000");
    }

    @Test
    public void testUserAdminConfigIsAppended() {
        Kafka resource = withAdminConfig(Map.of("default.api.timeout.ms", 10000, "request.timeout.ms", 5000));
        List<KafkaClusterSecurityContext> contexts = List.of(
                new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration()),
                new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), serviceAccountAuthentication()),
                new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration()));

        for (KafkaClusterSecurityContext securityContext : contexts) {
            String withoutUserConfig = Sova.fromCrd(new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                    KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext).kafkaAdminClientConfiguration();
            String withUserConfig = Sova.fromCrd(new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                    resource, VERSIONS, SHARED_ENV_PROVIDER, securityContext).kafkaAdminClientConfiguration();

            assertThat(withUserConfig, startsWith(withoutUserConfig));
            assertThat(withUserConfig.substring(withoutUserConfig.length()).lines().sorted().toList(),
                    contains("default.api.timeout.ms=10000", "request.timeout.ms=5000"));
        }
    }

    @Test
    public void testForbiddenAdminConfigIsIgnored() {
        Kafka resource = withAdminConfig(Map.of(
                "bootstrap.servers", "evil:9092",
                "security.protocol", "PLAINTEXT",
                "sasl.mechanism", "PLAIN",
                "ssl.truststore.location", "/x",
                "config.providers", "x"));
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration());

        String withoutUserConfig = Sova.fromCrd(new Reconciliation("test", KAFKA.getKind(), NAMESPACE, CLUSTER_NAME),
                KAFKA, VERSIONS, SHARED_ENV_PROVIDER, securityContext).kafkaAdminClientConfiguration();
        String withUserConfig = Sova.fromCrd(new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, securityContext).kafkaAdminClientConfiguration();

        assertThat(withUserConfig, is(withoutUserConfig));
    }

    @Test
    public void testAllowedSslExceptionPassesThrough() {
        Kafka resource = withAdminConfig(Map.of("ssl.endpoint.identification.algorithm", ""));
        Sova sova = Sova.fromCrd(new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, new KafkaClusterSecurityContext(
                        new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration()));

        assertThat(sova.kafkaAdminClientConfiguration().lines().toList(), hasItem("ssl.endpoint.identification.algorithm="));
    }

    @Test
    public void testCollectionIntervalFieldWinsOverTemplateEnv() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withCollectionIntervalMs(5000L)
                        .editTemplate()
                            .withNewContainer()
                                .withEnv(new ContainerEnvVarBuilder().withName(Sova.ENV_VAR_COLLECT_INTERVAL_MS).withValue("9999").build())
                            .endContainer()
                        .endTemplate()
                    .endExtraMetrics()
                .endSpec()
                .build();
        Sova sova = Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<String> values = sova.getEnvVars().stream()
                .filter(v -> Sova.ENV_VAR_COLLECT_INTERVAL_MS.equals(v.getName()))
                .map(EnvVar::getValue)
                .toList();
        assertThat(values, contains("5000"));
    }

    @Test
    public void testInvalidRegexIsRejected() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withGroupRegex("[")
                    .endExtraMetrics()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT));
        assertThat(e.getMessage(), containsString("Kafka.spec.extraMetrics.groupRegex"));
    }

    @Test
    public void testAllInvalidRegexesAreReported() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withTopicRegex("(")
                        .withTopicExcludeRegex("*")
                    .endExtraMetrics()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> Sova.fromCrd(
                new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT));
        assertThat(e.getMessage(), containsString("Kafka.spec.extraMetrics.topicRegex"));
        assertThat(e.getMessage(), containsString("Kafka.spec.extraMetrics.topicExcludeRegex"));
    }

    @Test
    public void testUserAdminConfigCannotInjectForbiddenOptions() throws Exception {
        Kafka resource = withAdminConfig(Map.of(
                "client.id", "x\nbootstrap.servers=evil:9093\nconfig.providers=evil",
                " security.protocol", "PLAINTEXT"));
        KafkaClusterSecurityContext securityContext = new KafkaClusterSecurityContext(
                new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration());
        Sova sova = Sova.fromCrd(new Reconciliation("test", resource.getKind(), NAMESPACE, CLUSTER_NAME),
                resource, VERSIONS, SHARED_ENV_PROVIDER, securityContext);

        // Sova parses the configuration with java.util.Properties, so check what it would actually see
        Properties parsed = new Properties();
        parsed.load(new StringReader(sova.kafkaAdminClientConfiguration()));

        assertThat(parsed.getProperty("bootstrap.servers"), is(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT));
        assertThat(parsed.getProperty("config.providers"), is("strimzisecrets"));
        assertThat(parsed.getProperty("security.protocol"), is("SSL"));
        assertThat(parsed.getProperty("client.id"), is("x\nbootstrap.servers=evil:9093\nconfig.providers=evil"));
    }

    // Utility methods

    private static Kafka withAdminConfig(Map<String, Object> config) {
        return new KafkaBuilder(KAFKA)
                .editSpec()
                    .editExtraMetrics()
                        .withConfig(config)
                    .endExtraMetrics()
                .endSpec()
                .build();
    }

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

    private static Volume getVolume(PodSpec podSpec, String volumeName) {
        return podSpec.getVolumes().stream().filter(volume -> volumeName.equals(volume.getName())).iterator().next();
    }

    private static VolumeMount getVolumeMount(Container container, String volumeName) {
        return container.getVolumeMounts().stream().filter(volumeMount -> volumeName.equals(volumeMount.getName())).iterator().next();
    }
}
