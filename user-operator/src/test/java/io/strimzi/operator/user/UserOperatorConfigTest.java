/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UserOperatorConfigTest {
    private static final Map<String, String> ENV_VARS = new HashMap<>(8);
    private static final Labels EXPECTED_LABELS;

    static {
        ENV_VARS.put(UserOperatorConfig.NAMESPACE.key(), "namespace");
        ENV_VARS.put(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key(), "30000");
        ENV_VARS.put(UserOperatorConfig.LABELS.key(), "label1=value1,label2=value2");
        ENV_VARS.put(UserOperatorConfig.CA_CERT_SECRET_NAME.key(), "ca-secret-cert");
        ENV_VARS.put(UserOperatorConfig.CA_KEY_SECRET_NAME.key(), "ca-secret-key");
        ENV_VARS.put(UserOperatorConfig.CA_NAMESPACE.key(), "differentnamespace");
        ENV_VARS.put(UserOperatorConfig.CERTS_VALIDITY_DAYS.key(), "1000");
        ENV_VARS.put(UserOperatorConfig.CERTS_RENEWAL_DAYS.key(), "10");
        ENV_VARS.put(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED.key(), "false");
        ENV_VARS.put(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key(), "20");


        Map<String, String> labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        EXPECTED_LABELS = Labels.fromMap(labels);
    }

    @Test
    public void testFromMap()    {
        UserOperatorConfig config = UserOperatorConfig.buildFromMap(ENV_VARS);

        assertThat(config.getNamespace(), is(ENV_VARS.get(UserOperatorConfig.NAMESPACE.key())));
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(ENV_VARS.get(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key()))));
        assertThat(config.getLabels(), is(EXPECTED_LABELS));
        assertThat(config.getCaCertSecretName(), is(ENV_VARS.get(UserOperatorConfig.CA_CERT_SECRET_NAME.key())));
        assertThat(config.getCaNamespaceOrNamespace(), is(ENV_VARS.get(UserOperatorConfig.CA_NAMESPACE.key())));
        assertThat(config.getClientsCaValidityDays(), is(1000));
        assertThat(config.getClientsCaRenewalDays(), is(10));
        assertThat(config.isAclsAdminApiSupported(), is(false));
        assertThat(config.getScramPasswordLength(), is(20));
        assertThat(config.getMaintenanceWindows(), is(nullValue()));
        assertThat(config.getOperationTimeoutMs(), is(300_000L));
        assertThat(config.getWorkQueueSize(), is(1_024));
        assertThat(config.getControllerThreadPoolSize(), is(50));
        assertThat(config.getCacheRefresh(), is(15_000L));
        assertThat(config.getBatchQueueSize(), is(1_024));
        assertThat(config.getBatchMaxBlockSize(), is(100));
        assertThat(config.getBatchMaxBlockTime(), is(100));
        assertThat(config.getUserOperationsThreadPoolSize(), is(4));
    }

    @Test
    public void testFromMapNamespaceEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.NAMESPACE.key());

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapCaNameEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.CA_CERT_SECRET_NAME.key());

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapReconciliationIntervalEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key());

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.defaultValue())));
    }

    @Test
    public void testFromMapScramPasswordLengthEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key());

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH), is(Integer.parseInt(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.defaultValue())));
    }

    @Test
    public void testFromMapStrimziLabelsEnvVarMissingSetsEmptyLabels()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.LABELS.key());

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.LABELS), is(Labels.EMPTY));
    }

    @Test
    public void testFromMapCaNamespaceMissingUsesNamespaceInstead()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.CA_NAMESPACE.key());

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.CA_NAMESPACE), is(envVars.get(UserOperatorConfig.CA_NAMESPACE.key())));
    }

    @Test
    public void testFromMapInvalidReconciliationIntervalThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key(), "not_a_long");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapInvalidScramPasswordLengthThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key(), "not_an_integer");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapInvalidLabelsStringThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.LABELS.key(), ",label1=");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapCaValidityRenewalEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.CERTS_VALIDITY_DAYS.key());
        envVars.remove(UserOperatorConfig.CERTS_RENEWAL_DAYS.key());

        UserOperatorConfig config =  UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.CERTS_VALIDITY_DAYS), is(Integer.parseInt(UserOperatorConfig.CERTS_VALIDITY_DAYS.defaultValue())));
        assertThat(config.get(UserOperatorConfig.CERTS_RENEWAL_DAYS), is(Integer.parseInt(UserOperatorConfig.CERTS_RENEWAL_DAYS.defaultValue())));
    }

    @Test
    public void testFromMapAclsAdminApiSupportedDefaults()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED.key());

        UserOperatorConfig config =  UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED), is(Boolean.parseBoolean(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED.defaultValue())));
    }

    @Test
    public void testMaintenanceTimeWindows()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);

        envVars.put(UserOperatorConfig.MAINTENANCE_TIME_WINDOWS.key(), "* * 8-10 * * ?;* * 14-15 * * ?");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.getMaintenanceWindows(), is(List.of("* * 8-10 * * ?", "* * 14-15 * * ?")));
    }

    @Test
    public void testKafkaAdminClientConfiguration() {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);

        envVars.put(UserOperatorConfig.KAFKA_ADMIN_CLIENT_CONFIGURATION.key(), "default.api.timeout.ms=13000\n" +
                "request.timeout.ms=130000");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);

        assertThat(config.getKafkaAdminClientConfiguration().get("default.api.timeout.ms"), is("13000"));
        assertThat(config.getKafkaAdminClientConfiguration().get("request.timeout.ms"), is("130000"));
    }

    @Test
    public void testParseNullKafkaAdminClientConfiguration() {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);

        envVars.put(UserOperatorConfig.MAINTENANCE_TIME_WINDOWS.key(), null);

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);

        assertThat(config.getKafkaAdminClientConfiguration().isEmpty(), is(true));
    }

    @Test
    public void testWorkQueueSize()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.WORK_QUEUE_SIZE.key(), "2048");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.WORK_QUEUE_SIZE), is(2048));
    }

    @Test
    public void testInvalidWorkQueueSize()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.WORK_QUEUE_SIZE.key(), "abcdefg");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testOperationTimeout()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.OPERATION_TIMEOUT_MS.key(), "120000");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.OPERATION_TIMEOUT_MS), is(120000L));
    }

    @Test
    public void testInvalidOperationTimeout()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.OPERATION_TIMEOUT_MS.key(), "abcdefg");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }
}
