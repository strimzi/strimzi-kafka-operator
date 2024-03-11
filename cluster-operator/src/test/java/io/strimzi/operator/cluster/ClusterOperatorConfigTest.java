/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.strimzi.operator.cluster.leaderelection.LeaderElectionManagerConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.UnsupportedVersionException;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClusterOperatorConfigTest {

    private static final Map<String, String> ENV_VARS = new HashMap<>(8);
    static {
        ENV_VARS.put(ClusterOperatorConfig.NAMESPACE.key(), "namespace");
        ENV_VARS.put(ClusterOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.key(), "30000");
        ENV_VARS.put(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "30000");
        ENV_VARS.put(ClusterOperatorConfig.CONNECT_BUILD_TIMEOUT_MS.key(), "40000");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.OPERATOR_NAMESPACE.key(), "operator-namespace");
        ENV_VARS.put(ClusterOperatorConfig.FEATURE_GATES.key(), "-UseKRaft");
        ENV_VARS.put(ClusterOperatorConfig.DNS_CACHE_TTL.key(), "10");
        ENV_VARS.put(ClusterOperatorConfig.POD_SECURITY_PROVIDER_CLASS.key(), "my.package.CustomPodSecurityProvider");
    }

    @Test
    public void testDefaultConfig() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.remove(ClusterOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.key());
        envVars.remove(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key());
        envVars.remove(ClusterOperatorConfig.CONNECT_BUILD_TIMEOUT_MS.key());
        envVars.remove(ClusterOperatorConfig.FEATURE_GATES.key());
        envVars.remove(ClusterOperatorConfig.POD_SECURITY_PROVIDER_CLASS.key());

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(ClusterOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.defaultValue())));
        assertThat(config.getOperationTimeoutMs(), is(Long.parseLong(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.defaultValue())));
        assertThat(config.getConnectBuildTimeoutMs(), is(Long.parseLong(ClusterOperatorConfig.CONNECT_BUILD_TIMEOUT_MS.defaultValue())));
        assertThat(config.getOperatorNamespace(), is("operator-namespace"));
        assertThat(config.getOperatorNamespaceLabels(), is(nullValue()));
        assertThat(config.featureGates().useKRaftEnabled(), is(true));
        assertThat(config.isCreateClusterRoles(), is(false));
        assertThat(config.isNetworkPolicyGeneration(), is(true));
        assertThat(config.isPodSetReconciliationOnly(), is(false));
        assertThat(config.getPodSecurityProviderClass(), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_CLASS.defaultValue()));
        assertThat(config.getLeaderElectionConfig(), is(nullValue()));
    }

    @Test
    public void testReconciliationInterval() {
        ClusterOperatorConfig config = new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                .with(ClusterOperatorConfig.NAMESPACE.key(), "namespace")
                .with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "30000")
                .with(ClusterOperatorConfig.ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS.key(), "20000")
                .with(ClusterOperatorConfig.CONNECT_BUILD_TIMEOUT_MS.key(), "120000")
                .with(ClusterOperatorConfig.DNS_CACHE_TTL.key(), "10")
                .build();

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(120_000L));
        assertThat(config.getOperationTimeoutMs(), is(30_000L));
        assertThat(config.getZkAdminSessionTimeoutMs(), is(20_000));
        assertThat(config.getConnectBuildTimeoutMs(), is(120_000L));
        assertThat(config.getDnsCacheTtlSec(), is(10));
    }

    @Test
    public void testEnvVars() {
        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(ENV_VARS, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(30_000L));
        assertThat(config.getOperationTimeoutMs(), is(30_000L));
        assertThat(config.getConnectBuildTimeoutMs(), is(40_000L));
        assertThat(config.getOperatorNamespace(), is("operator-namespace"));
        assertThat(config.featureGates().useKRaftEnabled(), is(false));
        assertThat(config.getDnsCacheTtlSec(), is(10));
        assertThat(config.getPodSecurityProviderClass(), is("my.package.CustomPodSecurityProvider"));
    }

    @Test
    public void testEnvVarsDefault() {
        Map<String, String> envVars = envWithImages();
        envVars.put(ClusterOperatorConfig.NAMESPACE.key(), "namespace");

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(ClusterOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.defaultValue())));
        assertThat(config.getOperationTimeoutMs(), is(Long.parseLong(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.defaultValue())));
        assertThat(config.getOperatorNamespace(), is(nullValue()));
        assertThat(config.getOperatorNamespaceLabels(), is(nullValue()));
        assertThat(config.getDnsCacheTtlSec(), is(Integer.parseInt(ClusterOperatorConfig.DNS_CACHE_TTL.defaultValue())));
        assertThat(config.getPodSecurityProviderClass(), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_CLASS.defaultValue()));
    }

    private Map<String, String> envWithImages() {
        Map<String, String> envVars = new HashMap<>(5);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
        return envVars;
    }

    @Test
    public void testListOfNamespaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.NAMESPACE.key(), "foo,bar,baz");

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("foo", "bar", "baz"))));
    }

    @Test
    public void testListOfNamespacesWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.NAMESPACE.key(), " foo ,bar , baz ");

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("foo", "bar", "baz"))));
    }

    @Test
    public void testNoNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.remove(ClusterOperatorConfig.NAMESPACE.key());

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testMinimalEnvVars() {
        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envWithImages(), KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testAnyNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.NAMESPACE.key(), "*");

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testAnyNamespaceWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.NAMESPACE.key(), " * ");

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testAnyNamespaceInList() {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
            envVars.put(ClusterOperatorConfig.NAMESPACE.key(), "foo,*,bar,baz");

            ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        });
    }

    @Test
    public void testImagePullPolicyWithEnvVarNotDefined() {
        assertThat(ClusterOperatorConfig.buildFromMap(ENV_VARS, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(nullValue()));
    }

    @Test
    public void testImagePullPolicyWithValidValues() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "Always");
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "IfNotPresent");
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT));

        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "Never");
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.NEVER));
    }

    @Test
    public void testImagePullPolicyWithUpperLowerCase() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "ALWAYS");
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "always");
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "Always");
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));
    }

    @Test
    public void testInvalidImagePullPolicyThrowsInvalidConfigurationException() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.IMAGE_PULL_POLICY.key(), "Sometimes");

        assertThrows(InvalidConfigurationException.class, () ->
            ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup())
        );
    }

    @Test
    public void testImagePullSecrets() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.IMAGE_PULL_SECRETS.key(), "secret1,  secret2 ,  secret3    ");

        List<LocalObjectReference> imagePullSecrets = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets();
        assertThat(imagePullSecrets, hasSize(3));
        assertThat(imagePullSecrets, hasItems(new LocalObjectReferenceBuilder().withName("secret1").build(),
            new LocalObjectReferenceBuilder().withName("secret2").build(),
            new LocalObjectReferenceBuilder().withName("secret3").build()));
    }

    @Test
    public void testImagePullSecretsThrowsWithInvalidCharacter() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.IMAGE_PULL_SECRETS.key(), "secret1, secret2 , secret_3 ");

        assertThrows(InvalidConfigurationException.class, () ->
            ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup())
        );
    }

    @Test
    public void testImagePullSecretsThrowsWithUpperCaseCharacter() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.IMAGE_PULL_SECRETS.key(), "Secret");

        assertThrows(InvalidConfigurationException.class, () ->
            ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup())
        );
    }

    @Test
    public void testConfigParsingWithAllVersionEnvVars() {
        Map<String, String> envVars = new HashMap<>(5);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());

        assertDoesNotThrow(() -> ClusterOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testConfigParsingWithMissingEnvVar() {
        Map<String, String> envVars = new HashMap<>(5);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());

        for (Map.Entry<String, String> envVar : envVars.entrySet())    {
            Map<String, String> editedEnvVars = new HashMap<>(envVars);
            editedEnvVars.remove(envVar.getKey());

            InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.buildFromMap(editedEnvVars));
            assertThat(e.getMessage(), containsString(envVar.getKey()));
        }
    }

    @Test
    public void testConfigUnsupportedVersions() {
        Map<String, String> envVars = new HashMap<>(5);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString() + " 2.6.0=myimage:2.6.0");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());

        Throwable exception = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.buildFromMap(envVars));
        assertThat(exception.getMessage(), is("Failed to parse default container image configuration for Kafka from environment variable STRIMZI_KAFKA_IMAGES"));
        assertThat(exception.getCause().getClass(), is(UnsupportedVersionException.class));
        assertThat(exception.getCause().getMessage(), is("Kafka version 2.6.0 has a container image configured but is not supported."));
    }

    @Test
    public void testOperatorNamespaceLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.OPERATOR_NAMESPACE_LABELS.key(), "nsLabelKey1=nsLabelValue1,nsLabelKey2=nsLabelValue2");

        Map<String, String> expectedLabels = new HashMap<>(2);
        expectedLabels.put("nsLabelKey1", "nsLabelValue1");
        expectedLabels.put("nsLabelKey2", "nsLabelValue2");

        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getOperatorNamespaceLabels(), is(Labels.fromMap(expectedLabels)));
    }

    @Test
    public void testInvalidOperatorNamespaceLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.OPERATOR_NAMESPACE_LABELS.key(), "nsLabelKey1,nsLabelKey2");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()));
        assertThat(e.getMessage(), containsString("Failed to parse. Value nsLabelKey1,nsLabelKey2 is not valid"));
    }

    @Test
    public void testInvalidFeatureGate() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.FEATURE_GATES.key(), "-NonExistingGate");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()));
        assertThat(e.getMessage(), containsString("Unknown feature gate NonExistingGate found in the configuration"));
    }

    @Test
    public void testCustomResourceSelectorLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.CUSTOM_RESOURCE_SELECTOR.key(), "nsLabelKey1=nsLabelValue1,nsLabelKey2=nsLabelValue2");

        Map<String, String> expectedLabels = new HashMap<>(2);
        expectedLabels.put("nsLabelKey1", "nsLabelValue1");
        expectedLabels.put("nsLabelKey2", "nsLabelValue2");

        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getCustomResourceSelector(), is(Labels.fromMap(expectedLabels)));
    }

    @Test
    public void testInvalidCustomResourceSelectorLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.CUSTOM_RESOURCE_SELECTOR.key(), "nsLabelKey1,nsLabelKey2");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()));
        assertThat(e.getMessage(), containsString("Failed to parse. Value nsLabelKey1,nsLabelKey2 is not valid"));
    }

    @Test
    public void testParsePodSecurityProviderClass() {
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("Baseline"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_BASELINE_CLASS.defaultValue()));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("baseline"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_BASELINE_CLASS.defaultValue()));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("RESTRICTED"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_RESTRICTED_CLASS.defaultValue()));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("restricted"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_RESTRICTED_CLASS.defaultValue()));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("my.package.MyClass"), is("my.package.MyClass"));
    }

    @Test
    public void testLeaderElectionConfig() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.LEADER_ELECTION_ENABLED.key(), "true");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME.key(), "my-lease");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE.key(), "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY.key(), "my-pod");

        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        config.getLeaderElectionConfig();
        assertThat(ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getLeaderElectionConfig(), is(notNullValue()));
    }
}
