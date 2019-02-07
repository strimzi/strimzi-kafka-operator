/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClusterOperatorConfigTest {

    private static Map<String, String> envVars = new HashMap<>(4);

    static {
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(ClusterOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, "30000");
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

        ClusterOperatorConfig config = new ClusterOperatorConfig(singleton("namespace"), 60_000, 30_000, false, new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()), null);

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

        Map<String, String> envVars = new HashMap<>(2);
        envVars.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, "namespace");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(ClusterOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, config.getReconciliationIntervalMs());
        assertEquals(ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, config.getOperationTimeoutMs());
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

    @Test(expected = InvalidConfigurationException.class)
    public void testNoNamespace() {

        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.remove(ClusterOperatorConfig.STRIMZI_NAMESPACE);

        ClusterOperatorConfig.fromMap(envVars);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testEmptyEnvVars() {

        ClusterOperatorConfig.fromMap(Collections.emptyMap());
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
        assertEquals("Always", ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "IfNotPresent");
        assertEquals("IfNotPresent", ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Never");
        assertEquals("Never", ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());
    }

    @Test
    public void testImagePullPolicyUpperLowerCase() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "ALWAYS");
        assertEquals("Always", ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "always");
        assertEquals("Always", ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());

        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Always");
        assertEquals("Always", ClusterOperatorConfig.fromMap(envVars).getImagePullPolicy());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidImagePullPolicy() {
        Map<String, String> envVars = new HashMap<>(ClusterOperatorConfigTest.envVars);
        envVars.put(ClusterOperatorConfig.STRIMZI_IMAGE_PULL_POLICY, "Sometimes");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);
    }
}
