/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.strimzi.controller.cluster.resources.Labels;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class ClusterControllerConfigTest {

    private static Labels labels;
    private static Map<String, String> envVars = new HashMap<>(4);

    static {
        labels = Labels.kind("cluster");

        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS, "strimzi.io/kind=cluster");
        envVars.put(ClusterControllerConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(ClusterControllerConfig.STRIMZI_OPERATION_TIMEOUT_MS, "30000");
    }

    @Test
    public void testDefaultConfig() {

        ClusterControllerConfig config = new ClusterControllerConfig(singleton("namespace"), labels);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(ClusterControllerConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, config.getReconciliationIntervalMs());
        assertEquals(ClusterControllerConfig.DEFAULT_OPERATION_TIMEOUT_MS, config.getOperationTimeoutMs());
    }

    @Test
    public void testReconciliationInterval() {

        ClusterControllerConfig config = new ClusterControllerConfig(singleton("namespace"), labels, 60_000, 30_000);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(60_000, config.getReconciliationIntervalMs());
        assertEquals(30_000, config.getOperationTimeoutMs());
    }

    @Test
    public void testEnvVars() {

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(30_000, config.getReconciliationIntervalMs());
        assertEquals(30_000, config.getOperationTimeoutMs());
    }

    @Test
    public void testEnvVarsDefault() {

        Map<String, String> envVars = new HashMap<>(2);
        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS, "strimzi.io/kind=cluster");

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(ClusterControllerConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, config.getReconciliationIntervalMs());
        assertEquals(ClusterControllerConfig.DEFAULT_OPERATION_TIMEOUT_MS, config.getOperationTimeoutMs());
    }

    @Test
    public void testListOfNamespaces() {

        Map<String, String> envVars = new HashMap<>(ClusterControllerConfigTest.envVars);
        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "foo, bar ,, baz , ");

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);
        assertEquals(new HashSet<>(asList("foo", "bar", "baz")), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(30_000, config.getReconciliationIntervalMs());
        assertEquals(30_000, config.getOperationTimeoutMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoNamespace() {

        Map<String, String> envVars = new HashMap<>(ClusterControllerConfigTest.envVars);
        envVars.remove(ClusterControllerConfig.STRIMZI_NAMESPACE);

        ClusterControllerConfig.fromMap(envVars);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoLabels() {

        Map<String, String> envVars = new HashMap<>(ClusterControllerConfigTest.envVars);
        envVars.remove(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS);

        ClusterControllerConfig.fromMap(envVars);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyEnvVars() {

        ClusterControllerConfig.fromMap(Collections.emptyMap());
    }
}
