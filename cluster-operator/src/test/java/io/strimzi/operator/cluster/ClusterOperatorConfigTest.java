/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.strimzi.operator.cluster.leaderelection.LeaderElectionManagerConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.UnsupportedVersionException;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
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
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, "30000");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_CONNECT_BUILD_TIMEOUT_MS, "40000");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_OPERATOR_NAMESPACE, "operator-namespace");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_FEATURE_GATES, "-UseStrimziPodSets");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_DNS_CACHE_TTL, "10");
        ENV_VARS.put(ClusterOperatorConfig.STRIMZI_POD_SECURITY_PROVIDER_CLASS, "my.package.CustomPodSecurityProvider");
    }

    @Test
    public void testDefaultConfig() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_CONNECT_BUILD_TIMEOUT_MS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_FEATURE_GATES);
        envVars.remove(ClusterOperatorConfig.STRIMZI_POD_SECURITY_PROVIDER_CLASS);

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
        assertThat(config.getOperationTimeoutMs(), is(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS));
        assertThat(config.getConnectBuildTimeoutMs(), is(ClusterOperatorConfig.DEFAULT_CONNECT_BUILD_TIMEOUT_MS));
        assertThat(config.getOperatorNamespace(), is("operator-namespace"));
        assertThat(config.getOperatorNamespaceLabels(), is(nullValue()));
        assertThat(config.featureGates().useStrimziPodSetsEnabled(), is(true));
        assertThat(config.featureGates().useKRaftEnabled(), is(false));
        assertThat(config.isCreateClusterRoles(), is(false));
        assertThat(config.isNetworkPolicyGeneration(), is(true));
        assertThat(config.isPodSetReconciliationOnly(), is(false));
        assertThat(config.getPodSecurityProviderClass(), is(ClusterOperatorConfig.DEFAULT_POD_SECURITY_PROVIDER_CLASS));
        assertThat(config.getLeaderElectionConfig(), is(nullValue()));
    }

    @Test
    public void testReconciliationInterval() {
        ClusterOperatorConfig config = new ClusterOperatorConfig(
                singleton("namespace"),
                60_000,
                30_000,
                120_000,
                false,
                true,
                new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()),
                null,
                null,
                null,
                null,
                null,
                "",
                10,
                20_000,
                10,
                false,
                1024,
                "operator_name",
                null, null);

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(60_000L));
        assertThat(config.getOperationTimeoutMs(), is(30_000L));
        assertThat(config.getZkAdminSessionTimeoutMs(), is(20_000));
        assertThat(config.getConnectBuildTimeoutMs(), is(120_000L));
        assertThat(config.getDnsCacheTtlSec(), is(10));
    }

    @Test
    public void testEnvVars() {
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(ENV_VARS, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(30_000L));
        assertThat(config.getOperationTimeoutMs(), is(30_000L));
        assertThat(config.getConnectBuildTimeoutMs(), is(40_000L));
        assertThat(config.getOperatorNamespace(), is("operator-namespace"));
        assertThat(config.featureGates().useStrimziPodSetsEnabled(), is(false));
        assertThat(config.getDnsCacheTtlSec(), is(10));
        assertThat(config.getPodSecurityProviderClass(), is("my.package.CustomPodSecurityProvider"));
    }

    @Test
    public void testEnvVarsDefault() {
        Map<String, String> envVars = envWithImages();
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
        assertThat(config.getOperationTimeoutMs(), is(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS));
        assertThat(config.getOperatorNamespace(), is(nullValue()));
        assertThat(config.getOperatorNamespaceLabels(), is(nullValue()));
        assertThat(config.getDnsCacheTtlSec(), is(ClusterOperatorConfig.DEFAULT_DNS_CACHE_TTL));
        assertThat(config.getPodSecurityProviderClass(), is(ClusterOperatorConfig.DEFAULT_POD_SECURITY_PROVIDER_CLASS));
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
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "foo,bar,baz");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("foo", "bar", "baz"))));
    }

    @Test
    public void testListOfNamespacesWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, " foo ,bar , baz ");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("foo", "bar", "baz"))));
    }

    @Test
    public void testNoNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_NAMESPACE);

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testMinimalEnvVars() {
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envWithImages(), KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testAnyNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "*");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testAnyNamespaceWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, " * ");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(List.of("*"))));
    }

    @Test
    public void testAnyNamespaceInList() {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
            envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "foo,*,bar,baz");

            ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        });
    }

    @Test
    public void testImagePullPolicyWithEnvVarNotDefined() {
        assertThat(ClusterOperatorConfig.fromMap(ENV_VARS, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(nullValue()));
    }

    @Test
    public void testImagePullPolicyWithValidValues() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "IfNotPresent");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Never");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.NEVER));
    }

    @Test
    public void testImagePullPolicyWithUpperLowerCase() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "ALWAYS");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "always");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));
    }

    @Test
    public void testInvalidImagePullPolicyThrowsInvalidConfigurationException() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Sometimes");

        assertThrows(InvalidConfigurationException.class, () ->
            ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup())
        );
    }

    @Test
    public void testImagePullSecrets() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "secret1,  secret2 ,  secret3    ");

        List<LocalObjectReference> imagePullSecrets = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets();
        assertThat(imagePullSecrets, hasSize(3));
        assertThat(imagePullSecrets, hasItems(new LocalObjectReferenceBuilder().withName("secret1").build(),
            new LocalObjectReferenceBuilder().withName("secret2").build(),
            new LocalObjectReferenceBuilder().withName("secret3").build()));
    }

    @Test
    public void testImagePullSecretsThrowsWithInvalidCharacter() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "secret1, secret2 , secret_3 ");

        assertThrows(InvalidConfigurationException.class, () ->
            ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup())
        );
    }

    @Test
    public void testImagePullSecretsThrowsWithUpperCaseCharacter() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "Secret");

        assertThrows(InvalidConfigurationException.class, () ->
            ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup())
        );
    }

    @Test
    public void testConfigParsingWithAllVersionEnvVars() {
        Map<String, String> envVars = new HashMap<>(5);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());

        assertDoesNotThrow(() -> ClusterOperatorConfig.fromMap(envVars));
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

            InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.fromMap(editedEnvVars));
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

        Throwable exception = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.fromMap(envVars));
        assertThat(exception.getMessage(), is("Failed to parse default container image configuration for Kafka from environment variable STRIMZI_KAFKA_IMAGES"));
        assertThat(exception.getCause().getClass(), is(UnsupportedVersionException.class));
        assertThat(exception.getCause().getMessage(), is("Kafka version 2.6.0 has a container image configured but is not supported."));
    }

    @Test
    public void testOperatorNamespaceLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_OPERATOR_NAMESPACE_LABELS, "nsLabelKey1=nsLabelValue1,nsLabelKey2=nsLabelValue2");

        Map<String, String> expectedLabels = new HashMap<>(2);
        expectedLabels.put("nsLabelKey1", "nsLabelValue1");
        expectedLabels.put("nsLabelKey2", "nsLabelValue2");

        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getOperatorNamespaceLabels(), is(Labels.fromMap(expectedLabels)));
    }

    @Test
    public void testInvalidOperatorNamespaceLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_OPERATOR_NAMESPACE_LABELS, "nsLabelKey1,nsLabelKey2");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()));
        assertThat(e.getMessage(), containsString("Failed to parse labels from STRIMZI_OPERATOR_NAMESPACE_LABELS"));
    }

    @Test
    public void testInvalidFeatureGate() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_FEATURE_GATES, "-NonExistingGate");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()));
        assertThat(e.getMessage(), containsString("Unknown feature gate NonExistingGate found in the configuration"));
    }

    @Test
    public void testCustomResourceSelectorLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_CUSTOM_RESOURCE_SELECTOR, "nsLabelKey1=nsLabelValue1,nsLabelKey2=nsLabelValue2");

        Map<String, String> expectedLabels = new HashMap<>(2);
        expectedLabels.put("nsLabelKey1", "nsLabelValue1");
        expectedLabels.put("nsLabelKey2", "nsLabelValue2");

        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getCustomResourceSelector(), is(Labels.fromMap(expectedLabels)));
    }

    @Test
    public void testInvalidCustomResourceSelectorLabels() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_CUSTOM_RESOURCE_SELECTOR, "nsLabelKey1,nsLabelKey2");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()));
        assertThat(e.getMessage(), containsString("Failed to parse labels from STRIMZI_CUSTOM_RESOURCE_SELECTOR"));
    }

    @Test
    public void testParseBoolean() {
        assertThat(ClusterOperatorConfig.parseBoolean(null, true), is(true));
        assertThat(ClusterOperatorConfig.parseBoolean("true", true), is(true));
        assertThat(ClusterOperatorConfig.parseBoolean("false", true), is(false));
        assertThat(ClusterOperatorConfig.parseBoolean("FALSE", true), is(false));
    }

    @Test
    public void testParsePodSecurityProviderClass() {
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("Baseline"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_BASELINE_CLASS));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("baseline"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_BASELINE_CLASS));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("RESTRICTED"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_RESTRICTED_CLASS));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("restricted"), is(ClusterOperatorConfig.POD_SECURITY_PROVIDER_RESTRICTED_CLASS));
        assertThat(ClusterOperatorConfig.parsePodSecurityProviderClass("my.package.MyClass"), is("my.package.MyClass"));
    }

    @Test
    public void testLeaderElectionConfig() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.ENV_VARS);
        envVars.put(ClusterOperatorConfig.STRIMZI_LEADER_ELECTION_ENABLED, "true");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAME, "my-lease");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_LEASE_NAMESPACE, "my-namespace");
        envVars.put(LeaderElectionManagerConfig.ENV_VAR_LEADER_ELECTION_IDENTITY, "my-pod");

        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getLeaderElectionConfig(), is(notNullValue()));
    }
}
