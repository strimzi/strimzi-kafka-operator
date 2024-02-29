/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultAdminClientProviderTest {
    private static final Secret EMPTY_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withNamespace("my-namespace")
                .withName("empty-secret")
            .endMetadata()
            .build();
    private static final Secret INVALID_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withNamespace("my-namespace")
                .withName("invalid-secret")
            .endMetadata()
            .withData(Map.of("not.certificate", "dGhpcyBpcyBub3QgY2VydGlmaWNhdGU="))
            .build();
    private static final Secret CA_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withNamespace("my-namespace")
                .withName("ca-secret")
            .endMetadata()
            .withData(Map.of("ca1.crt", "Y2Ex", "ca2.crt", "Y2Ey"))
            .build();
    private static final Secret USER_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withNamespace("my-namespace")
                .withName("user-secret")
            .endMetadata()
            .withData(Map.of("user.key", "dXNlci1rZXk=", "user.crt", "dXNlci1jZXJ0"))
            .build();

    private void assertDefaultConfigs(Properties config) {
        assertThat(config.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), is("my-kafka:9092"));
        assertThat(config.get(AdminClientConfig.METADATA_MAX_AGE_CONFIG), is("30000"));
        assertThat(config.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), is("10000"));
        assertThat(config.get(AdminClientConfig.RETRIES_CONFIG), is("3"));
        assertThat(config.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), is("40000"));
    }

    @Test
    public void testPlainConnection() {
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, null, null, new Properties());

        assertThat(config.size(), is(5));
        assertDefaultConfigs(config);
    }

    @Test
    public void testCustomConfig() {
        Properties customConfig = new Properties();
        customConfig.setProperty(AdminClientConfig.RETRIES_CONFIG, "5"); // Override a value we have default for
        customConfig.setProperty(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, "13000"); // Override a value we do not use

        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, null, null, customConfig);

        assertThat(config.size(), is(6));
        assertThat(config.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), is("my-kafka:9092"));
        assertThat(config.get(AdminClientConfig.METADATA_MAX_AGE_CONFIG), is("30000"));
        assertThat(config.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), is("10000"));
        assertThat(config.get(AdminClientConfig.RETRIES_CONFIG), is("5"));
        assertThat(config.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), is("40000"));
        assertThat(config.get(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG), is("13000"));
    }

    @Test
    public void testTlsConnection() {
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", CA_SECRET, null, null, new Properties());

        assertThat(config.size(), is(8));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca1")); // The order is not deterministic. So we check both certificates are present
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca2"));
    }

    @Test
    public void testMTlsConnection() {
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", CA_SECRET, USER_SECRET, "user", new Properties());

        assertThat(config.size(), is(11));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca1")); // The order is not deterministic. So we check both certificates are present
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca2"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG).toString(), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).toString(), is("user-cert"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG).toString(), is("user-key"));
    }

    @Test
    public void testMTlsWithPublicCAConnection() {
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, USER_SECRET, "user", new Properties());

        assertThat(config.size(), is(9));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG).toString(), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).toString(), is("user-cert"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG).toString(), is("user-key"));
    }

    @Test
    public void testNullConfig() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, USER_SECRET, "user", null));
        assertThat(ex.getMessage(), is("The config parameter should not be null"));
    }

    @Test
    public void testInvalidCASecret() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", EMPTY_SECRET, USER_SECRET, "user", new Properties()));
        assertThat(ex.getMessage(), is("The Secret empty-secret does not seem to contain any .crt entries"));

        ex = assertThrows(InvalidConfigurationException.class, () -> DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", INVALID_SECRET, USER_SECRET, "user", new Properties()));
        assertThat(ex.getMessage(), is("The Secret invalid-secret does not seem to contain any .crt entries"));
    }

    @Test
    public void testInvalidUserSecret() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", CA_SECRET, EMPTY_SECRET, "user", new Properties()));
        assertThat(ex.getMessage(), is("The Secret empty-secret does not seem to contain user.key and user.crt entries"));

        ex = assertThrows(InvalidConfigurationException.class, () -> DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", CA_SECRET, INVALID_SECRET, "user", new Properties()));
        assertThat(ex.getMessage(), is("The Secret invalid-secret does not seem to contain user.key and user.crt entries"));
    }
}
