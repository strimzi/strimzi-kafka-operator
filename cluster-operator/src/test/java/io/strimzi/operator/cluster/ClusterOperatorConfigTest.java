/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClusterOperatorConfigTest {

    private static Map<String, String> envVars = new HashMap<>(4);

    static {
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, "30000");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, KafkaVersionTestUtils.getKafkaConnectS2iImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
    }

    @Test
    public void testDefaultConfig() {

        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.remove(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS);

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
        assertThat(config.getOperationTimeoutMs(), is(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS));
    }

    @Test
    public void testReconciliationInterval() {

        ClusterOperatorConfig config = new ClusterOperatorConfig(singleton("namespace"), 60_000, 30_000, false, new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap()), null, null);

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(60_000L));
        assertThat(config.getOperationTimeoutMs(), is(30_000L));
    }

    @Test
    public void testEnvVars() {

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(30_000L));
        assertThat(config.getOperationTimeoutMs(), is(30_000L));
    }

    @Test
    public void testEnvVarsDefault() {

        Map<String, String> envVars = envWithImages();
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        assertThat(config.getNamespaces(), is(singleton("namespace")));
        assertThat(config.getReconciliationIntervalMs(), is(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
        assertThat(config.getOperationTimeoutMs(), is(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS));
    }

    private Map<String, String> envWithImages() {
        Map<String, String> envVars = new HashMap<>(2);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, KafkaVersionTestUtils.getKafkaConnectS2iImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
        return envVars;
    }

    @Test
    public void testListOfNamespaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "foo,bar,baz");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("foo", "bar", "baz"))));
    }

    @Test
    public void testListOfNamespacesWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, " foo ,bar , baz ");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("foo", "bar", "baz"))));
    }

    @Test
    public void testNoNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.remove(ClusterOperatorConfig.STRIMZI_NAMESPACE);

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("*"))));
    }

    @Test
    public void testMinimalEnvVars() {
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envWithImages(), KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("*"))));
    }

    @Test
    public void testAnyNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "*");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("*"))));
    }

    @Test
    public void testAnyNamespaceWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, " * ");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        assertThat(config.getNamespaces(), is(new HashSet<>(asList("*"))));
    }

    @Test
    public void testAnyNamespaceInList() {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
            envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "foo,*,bar,baz");

            ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        });
    }

    @Test
    public void testImagePullPolicyNotDefined() {
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(nullValue()));
    }

    @Test
    public void testImagePullPolicyValidValues() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "IfNotPresent");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Never");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.NEVER));
    }

    @Test
    public void testImagePullPolicyUpperLowerCase() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "ALWAYS");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "always");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS));
    }

    @Test
    public void testInvalidImagePullPolicy() {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
            envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Sometimes");

            ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
        });
    }

    @Test
    public void testImagePullSecrets() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "secret1,  secret2 ,  secret3    ");
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets().size(), is(3));
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets().contains(new LocalObjectReferenceBuilder().withName("secret1").build()), is(true));
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets().contains(new LocalObjectReferenceBuilder().withName("secret2").build()), is(true));
        assertThat(ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets().contains(new LocalObjectReferenceBuilder().withName("secret3").build()), is(true));
    }

    @Test
    public void testImagePullSecretsInvalidCharacter() {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
            envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "secret1, secret2 , secret_3 ");
            ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets().size();
        });
    }

    @Test
    public void testImagePullSecretsUpperCaseCharacter() {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
            envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "Secret");
            ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup()).getImagePullSecrets().size();
        });
    }
}
