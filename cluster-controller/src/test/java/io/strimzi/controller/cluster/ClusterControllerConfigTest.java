/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.NamespaceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterControllerConfigTest {

    private static Map<String, String> labels = new HashMap<>(1);
    private static Map<String, String> envVars = new HashMap<>(3);

    static {
        labels.put("strimzi.io/kind", "cluster");

        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS, "strimzi.io/kind=cluster");
        envVars.put(ClusterControllerConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL, "30000");
    }

    @Test
    public void testDefaultConfig() {

        ClusterControllerConfig config = new ClusterControllerConfig(singleton("namespace"), labels);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(ClusterControllerConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL, config.getReconciliationInterval());
    }

    @Test
    public void testReconciliationInterval() {

        ClusterControllerConfig config = new ClusterControllerConfig(singleton("namespace"), labels, 60000);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(60_000, config.getReconciliationInterval());
    }

    @Test
    public void testEnvVars() {

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(30_000, config.getReconciliationInterval());
    }

    @Test
    public void testEnvVarsDefault() {

        Map<String, String> envVars = new HashMap<>(2);
        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS, "strimzi.io/kind=cluster");

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);

        assertEquals(singleton("namespace"), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(ClusterControllerConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL, config.getReconciliationInterval());
    }

    @Test
    public void testListOfNamespaces() {

        Map<String, String> envVars = new HashMap<>(ClusterControllerConfigTest.envVars);
        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "foo, bar ,, baz , ");

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);
        assertEquals(new HashSet(asList("foo","bar", "baz")), config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(30000, config.getReconciliationInterval());
    }

    @Test
    public void testNoNamespace() {

        Map<String, String> envVars = new HashMap<>(ClusterControllerConfigTest.envVars);
        envVars.remove(ClusterControllerConfig.STRIMZI_NAMESPACE);

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);
        assertEquals(null, config.getNamespaces());
        assertEquals(labels, config.getLabels());
        assertEquals(30000, config.getReconciliationInterval());
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
