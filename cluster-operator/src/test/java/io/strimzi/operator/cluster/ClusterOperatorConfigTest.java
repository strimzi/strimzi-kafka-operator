/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClusterOperatorConfigTest {

    private static Map<String, String> envVars = new HashMap<>(4);

    static {
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, "30000");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
    }

    @Test
    public void testDefaultConfig() {

        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.remove(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
        envVars.remove(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS);

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, config.getReconciliationIntervalMs());
        assertEquals(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, config.getOperationTimeoutMs());
    }

    @Test
    public void testReconciliationInterval() {

        ClusterOperatorConfig config = new ClusterOperatorConfig(singleton("namespace"), 60_000, 30_000, false, new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()), null, null);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(60_000, config.getReconciliationIntervalMs());
        assertEquals(30_000, config.getOperationTimeoutMs());
    }

    @Test
    public void testEnvVars() {

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(30_000, config.getReconciliationIntervalMs());
        assertEquals(30_000, config.getOperationTimeoutMs());
    }

    @Test
    public void testEnvVarsDefault() {

        Map<String, String> envVars = envWithImages();
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, config.getReconciliationIntervalMs());
        assertEquals(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, config.getOperationTimeoutMs());
    }

    private Map<String, String> envWithImages() {
        Map<String, String> envVars = new HashMap<>(2);
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        return envVars;
    }

    @Test
    public void testListOfNamespaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "foo,bar,baz");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
        assertEquals(new HashSet<>(asList("foo", "bar", "baz")), config.getNamespaces());
    }

    @Test
    public void testListOfNamespacesWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, " foo ,bar , baz ");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
        assertEquals(new HashSet<>(asList("foo", "bar", "baz")), config.getNamespaces());
    }

    @Test
    public void testNoNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.remove(ClusterOperatorConfig.STRIMZI_NAMESPACE);

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
        assertEquals(new HashSet<>(asList("*")), config.getNamespaces());
    }

    @Test
    public void testMinimalEnvVars() {
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envWithImages());
        assertEquals(new HashSet<>(asList("*")), config.getNamespaces());
    }

    @Test
    public void testAnyNamespace() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "*");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
        assertEquals(new HashSet<>(asList("*")), config.getNamespaces());
    }

    @Test
    public void testAnyNamespaceWithSpaces() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, " * ");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
        assertEquals(new HashSet<>(asList("*")), config.getNamespaces());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testAnyNamespaceInList() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "foo,*,bar,baz");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
    }

    @Test
    public void testImagePullPolicyNotDefined() {
        assertNull(ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());
    }

    @Test
    public void testImagePullPolicyValidValues() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertEquals(ImagePullPolicy.ALWAYS, ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "IfNotPresent");
        assertEquals(ImagePullPolicy.IFNOTPRESENT, ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Never");
        assertEquals(ImagePullPolicy.NEVER, ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());
    }

    @Test
    public void testImagePullPolicyUpperLowerCase() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "ALWAYS");
        assertEquals(ImagePullPolicy.ALWAYS, ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "always");
        assertEquals(ImagePullPolicy.ALWAYS, ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertEquals(ImagePullPolicy.ALWAYS, ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidImagePullPolicy() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Sometimes");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
    }

    @Test
    public void testImagePullSecrets() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "secret1,  secret2 ,  secret3    ");
        assertEquals(3, ClusterOperatorConfig.fromMap(envVars).getImagePullSecrets().size());
        assertTrue(ClusterOperatorConfig.fromMap(envVars).getImagePullSecrets().contains(new LocalObjectReferenceBuilder().withName("secret1").build()));
        assertTrue(ClusterOperatorConfig.fromMap(envVars).getImagePullSecrets().contains(new LocalObjectReferenceBuilder().withName("secret2").build()));
        assertTrue(ClusterOperatorConfig.fromMap(envVars).getImagePullSecrets().contains(new LocalObjectReferenceBuilder().withName("secret3").build()));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testImagePullSecretsInvalidCharacter() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "secret1, secret2 , secret_3 ");
        ClusterOperatorConfig.fromMap(envVars).getImagePullSecrets().size();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testImagePullSecretsUpperCaseCharacter() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_SECRETS, "Secret");
        ClusterOperatorConfig.fromMap(envVars).getImagePullSecrets().size();
    }
}
