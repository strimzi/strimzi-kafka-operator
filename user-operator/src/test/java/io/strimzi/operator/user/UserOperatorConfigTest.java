/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UserOperatorConfigTest {
    private static Map<String, String> envVars = new HashMap<>(8);
    private static Labels expectedLabels;

    static {
        envVars.put(UserOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, "label1=value1,label2=value2");
        envVars.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, "ca-secret-cert");
        envVars.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, "ca-secret-key");
        envVars.put(UserOperatorConfig.STRIMZI_CA_NAMESPACE, "differentnamespace");
        envVars.put(UserOperatorConfig.STRIMZI_ZOOKEEPER_CONNECT, "somehost:2181");
        envVars.put(UserOperatorConfig.STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS, "6000");

        Map labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        expectedLabels = Labels.fromMap(labels);
    }

    @Test
    public void testConfig()    {
        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);

        assertThat(config.getNamespace(), is(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE)));
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(envVars.get(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS))));
        assertThat(config.getLabels(), is(expectedLabels));
        assertThat(config.getCaCertSecretName(), is(envVars.get(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME)));
        assertThat(config.getCaNamespace(), is(envVars.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE)));
        assertThat(config.getZookeperConnect(), is(envVars.get(UserOperatorConfig.STRIMZI_ZOOKEEPER_CONNECT)));
        assertThat(config.getZookeeperSessionTimeoutMs(), is(Long.parseLong(envVars.get(UserOperatorConfig.STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS))));
    }

    @Test
    public void testNamespaceEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_NAMESPACE);

        assertThrows(InvalidConfigurationException.class, () -> {
            UserOperatorConfig.fromMap(envVars);
        });
    }

    @Test
    public void testMissingCaName()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME);

        assertThrows(InvalidConfigurationException.class, () -> {
            UserOperatorConfig.fromMap(envVars);
        });
    }

    @Test
    public void testMissingReconciliationInterval()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getReconciliationIntervalMs(), is(UserOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
    }

    @Test
    public void testMissingLabels()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_LABELS);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getLabels(), is(Labels.EMPTY));
    }

    @Test
    public void testMissingCaNamespace()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_NAMESPACE);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getCaNamespace(), is(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE)));
    }

    @Test
    public void testInvalidReconciliationInterval()  {
        assertThrows(NumberFormatException.class, () -> {
            Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
            envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "not_an_long");

            UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        });
    }

    @Test
    public void testInvalidLabels()  {
        assertThrows(InvalidConfigurationException.class, () -> {
            Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
            envVars.put(UserOperatorConfig.STRIMZI_LABELS, ",label1=");

            UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        });
    }
}
