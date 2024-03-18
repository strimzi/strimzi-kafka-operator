/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultAdminClientProviderTest {
    private static final String CA1 = "ca1";
    private static final String CA2 = "ca2";
    private static final String USER_CERT = "user-cert";
    private static final String USER_KEY = "user-key";

    private void assertDefaultConfigs(Properties config) {
        assertThat(config.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), is("my-kafka:9092"));
        assertThat(config.get(AdminClientConfig.METADATA_MAX_AGE_CONFIG), is("30000"));
        assertThat(config.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), is("10000"));
        assertThat(config.get(AdminClientConfig.RETRIES_CONFIG), is("3"));
        assertThat(config.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), is("40000"));
    }

    @Test
    public void testPlainConnection() {
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, null, new Properties());

        assertThat(config.size(), is(5));
        assertDefaultConfigs(config);
    }

    @Test
    public void testCustomConfig() {
        Properties customConfig = new Properties();
        customConfig.setProperty(AdminClientConfig.RETRIES_CONFIG, "5"); // Override a value we have default for
        customConfig.setProperty(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, "13000"); // Override a value we do not use

        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, null, customConfig);

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
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", mockPemTrustSet(), null, new Properties());

        assertThat(config.size(), is(8));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca1")); // The order is not deterministic. So we check both certificates are present
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca2"));
    }

    @Test
    public void testMTlsConnection() {
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", mockPemTrustSet(), mockPemAuthIdentity(), new Properties());

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
        Properties config = DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, mockPemAuthIdentity(), new Properties());

        assertThat(config.size(), is(9));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG).toString(), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).toString(), is("user-cert"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG).toString(), is("user-key"));
    }

    @Test
    public void testNullConfig() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> DefaultAdminClientProvider.adminClientConfiguration("my-kafka:9092", null, mockPemAuthIdentity(), null));
        assertThat(ex.getMessage(), is("The config parameter should not be null"));
    }

    public static PemTrustSet mockPemTrustSet() {
        PemTrustSet mockTrustSet = mock(PemTrustSet.class);
        when(mockTrustSet.trustedCertificatesString()).thenReturn(String.format("%s%n%s", CA1, CA2));
        return mockTrustSet;
    }

    public static PemAuthIdentity mockPemAuthIdentity() {
        PemAuthIdentity mockAuthIdentity = mock(PemAuthIdentity.class);
        when(mockAuthIdentity.certificateChainAsPem()).thenReturn(USER_CERT);
        when(mockAuthIdentity.privateKeyAsPem()).thenReturn(USER_KEY);
        return mockAuthIdentity;
    }
}
