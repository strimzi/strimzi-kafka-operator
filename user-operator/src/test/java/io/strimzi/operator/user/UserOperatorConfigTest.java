/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.api.kafka.model.CertificateAuthority;
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
    private static Map<String, String> envVars = new HashMap<>(8);
    private static Labels expectedLabels;

    static {
        envVars.put(UserOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, "label1=value1,label2=value2");
        envVars.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, "ca-secret-cert");
        envVars.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, "ca-secret-key");
        envVars.put(UserOperatorConfig.STRIMZI_CA_NAMESPACE, "differentnamespace");
        envVars.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY, "1000");
        envVars.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL, "10");
        envVars.put(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED, "false");
        envVars.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, "20");


        Map<String, String> labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        expectedLabels = Labels.fromMap(labels);
    }

    @Test
    public void testFromMap()    {
        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);

        assertThat(config.getNamespace(), is(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE)));
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(envVars.get(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS))));
        assertThat(config.getLabels(), is(expectedLabels));
        assertThat(config.getCaCertSecretName(), is(envVars.get(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME)));
        assertThat(config.getCaNamespace(), is(envVars.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE)));
        assertThat(config.getClientsCaValidityDays(), is(1000));
        assertThat(config.getClientsCaRenewalDays(), is(10));
        assertThat(config.isAclsAdminApiSupported(), is(false));
        assertThat(config.getScramPasswordLength(), is(20));
        assertThat(config.getMaintenanceWindows(), is(nullValue()));
    }

    @Test
    public void testFromMapNamespaceEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_NAMESPACE);

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.fromMap(envVars));
    }

    @Test
    public void testFromMapCaNameEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME);

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.fromMap(envVars));
    }

    @Test
    public void testFromMapReconciliationIntervalEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getReconciliationIntervalMs(), is(UserOperatorConfig.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
    }

    @Test
    public void testFromMapScramPasswordLengthEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getScramPasswordLength(), is(UserOperatorConfig.DEFAULT_SCRAM_SHA_PASSWORD_LENGTH));
    }

    @Test
    public void testFromMapStrimziLabelsEnvVarMissingSetsEmptyLabels()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_LABELS);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getLabels(), is(Labels.EMPTY));
    }

    @Test
    public void testFromMapCaNamespaceMissingUsesNamespaceInstead()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_NAMESPACE);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getCaNamespace(), is(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE)));
    }

    @Test
    public void testFromMapInvalidReconciliationIntervalThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "not_a_long");

        assertThrows(NumberFormatException.class, () -> UserOperatorConfig.fromMap(envVars));
    }

    @Test
    public void testFromMapInvalidScramPasswordLengthThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, "not_an_integer");

        assertThrows(NumberFormatException.class, () -> UserOperatorConfig.fromMap(envVars));
    }

    @Test
    public void testFromMapInvalidLabelsStringThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, ",label1=");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.fromMap(envVars));
    }

    @Test
    public void testFromMapCaValidityRenewalEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY);
        envVars.remove(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getClientsCaValidityDays(), is(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS));
        assertThat(config.getClientsCaRenewalDays(), is(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS));
    }

    @Test
    public void testFromMapAclsAdminApiSupportedDefaults()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);
        envVars.remove(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED);

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.isAclsAdminApiSupported(), is(UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED));
    }

    @Test
    public void testMaintenanceTimeWindows()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.envVars);

        envVars.put(UserOperatorConfig.STRIMZI_MAINTENANCE_TIME_WINDOWS, "* * 8-10 * * ?;* * 14-15 * * ?");

        UserOperatorConfig config = UserOperatorConfig.fromMap(envVars);
        assertThat(config.getMaintenanceWindows(), is(List.of("* * 8-10 * * ?", "* * 14-15 * * ?")));
    }

    @Test
    public void testParseMaintenanceTimeWindows()    {
        assertThat(UserOperatorConfig.parseMaintenanceTimeWindows("* * 8-10 * * ?;* * 14-15 * * ?"), is(List.of("* * 8-10 * * ?", "* * 14-15 * * ?")));
        assertThat(UserOperatorConfig.parseMaintenanceTimeWindows("* * 8-10 * * ?"), is(List.of("* * 8-10 * * ?")));
        assertThat(UserOperatorConfig.parseMaintenanceTimeWindows(null), is(nullValue()));
        assertThat(UserOperatorConfig.parseMaintenanceTimeWindows(""), is(nullValue()));
    }
}
