/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UserOperatorConfigTest {
    private static Map<String, String> envVars = new HashMap<>(5);
    private static Labels expectedLabels;

    static {
        envVars.put(UserOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, "label1=value1,label2=value2");
        envVars.put(UserOperatorConfig.STRIMZI_CA_NAME, "ca-secret");
        envVars.put(UserOperatorConfig.STRIMZI_CA_NAMESPACE, "differentnamespace");

        Map labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        expectedLabels = Labels.fromMap(labels);
    }

    @Test
    public void testConfig()    {
        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);

        assertEquals(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE), config.getNamespace());
        assertEquals(Long.parseLong(envVars.get(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS)), config.getReconciliationIntervalMs());
        assertEquals(expectedLabels, config.getLabels());
        assertEquals(envVars.get(UserOperatorConfig.STRIMZI_CA_NAME), config.getCaName());
        assertEquals(envVars.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE), config.getCaNamespace());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingNamespace()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_NAMESPACE);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingCaName()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_NAME);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
    }

    @Test
    public void testMissingReconciliationInterval()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertEquals(UserOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, config.getReconciliationIntervalMs());
    }

    @Test
    public void testMissingLabels()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_LABELS);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertEquals(Labels.EMPTY, config.getLabels());
    }

    @Test
    public void testMissingCaNamespace()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_NAMESPACE);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertEquals(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE), config.getCaNamespace());
    }

    @Test(expected = NumberFormatException.class)
    public void testInvalidReconciliationInterval()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "not_an_long");

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidLabels()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, ",label1=");

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
    }
}
