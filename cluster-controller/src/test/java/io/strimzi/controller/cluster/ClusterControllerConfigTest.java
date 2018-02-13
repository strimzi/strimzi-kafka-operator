/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import static org.junit.Assert.assertEquals;

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

        ClusterControllerConfig config = new ClusterControllerConfig("namespace", labels);

        assertEquals("namespace", config.getNamespace());
        assertEquals(labels, config.getLabels());
        assertEquals(ClusterControllerConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL, config.getReconciliationInterval());
    }

    @Test
    public void testReconciliationInterval() {

        ClusterControllerConfig config = new ClusterControllerConfig("namespace", labels, 60000);

        assertEquals("namespace", config.getNamespace());
        assertEquals(labels, config.getLabels());
        assertEquals(60_000, config.getReconciliationInterval());
    }

    @Test
    public void testEnvVars() {

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);

        assertEquals("namespace", config.getNamespace());
        assertEquals(labels, config.getLabels());
        assertEquals(30_000, config.getReconciliationInterval());
    }

    @Test
    public void testEnvVarsDefault() {

        Map<String, String> envVars = new HashMap<>(2);
        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS, "strimzi.io/kind=cluster");

        ClusterControllerConfig config = ClusterControllerConfig.fromMap(envVars);

        assertEquals("namespace", config.getNamespace());
        assertEquals(labels, config.getLabels());
        assertEquals(ClusterControllerConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL, config.getReconciliationInterval());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoNamespace() {

        Map<String, String> envVars = new HashMap<>(1);
        envVars.put(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS, "strimzi.io/kind=cluster");

        ClusterControllerConfig.fromMap(envVars);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoLabels() {

        Map<String, String> envVars = new HashMap<>(1);
        envVars.put(ClusterControllerConfig.STRIMZI_NAMESPACE, "namespace");

        ClusterControllerConfig.fromMap(envVars);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyEnvVars() {

        ClusterControllerConfig.fromMap(Collections.emptyMap());
    }
}
