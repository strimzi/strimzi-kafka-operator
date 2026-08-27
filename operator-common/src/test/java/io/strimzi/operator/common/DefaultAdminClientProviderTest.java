/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.ProjectedServiceAccountAuthIdentity;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultAdminClientProviderTest {
    private static final String CA1 = "ca1";
    private static final String CA2 = "ca2";
    private static final String USER_CERT = "user-cert";
    private static final String USER_KEY = "user-key";
    private static final String SA_TOKEN_PATH = "/var/run/secrets/kafka/serviceaccount/token";

    private void assertDefaultConfigs(Properties config) {
        assertThat(config.get(AdminClientConfig.METADATA_MAX_AGE_CONFIG), is("30000"));
        assertThat(config.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), is("10000"));
        assertThat(config.get(AdminClientConfig.RETRIES_CONFIG), is("3"));
        assertThat(config.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), is("40000"));
    }

    @Test
    public void testPlainConnection() {
        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(null, null, new Properties());

        assertThat(config.size(), is(4));
        assertDefaultConfigs(config);
    }

    @Test
    public void testCustomConfig() {
        Properties customConfig = new Properties();
        customConfig.setProperty(AdminClientConfig.RETRIES_CONFIG, "5"); // Override a value we have default for
        customConfig.setProperty(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, "13000"); // Override a value we do not use

        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(null, null, customConfig);

        assertThat(config.size(), is(5));
        assertThat(config.get(AdminClientConfig.METADATA_MAX_AGE_CONFIG), is("30000"));
        assertThat(config.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), is("10000"));
        assertThat(config.get(AdminClientConfig.RETRIES_CONFIG), is("5"));
        assertThat(config.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), is("40000"));
        assertThat(config.get(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG), is("13000"));
    }

    @Test
    public void testTlsConnection() {
        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(mockPemTrustSet(), null, new Properties());

        assertThat(config.size(), is(7));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca1")); // The order is not deterministic. So we check both certificates are present
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca2"));
    }

    @Test
    public void testMTlsConnection() {
        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(mockPemTrustSet(), mockPemAuthIdentity(), new Properties());

        assertThat(config.size(), is(10));
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
        Properties customConfig = new Properties();
        customConfig.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");

        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(null, mockPemAuthIdentity(), customConfig);

        assertThat(config.size(), is(8));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG).toString(), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).toString(), is("user-cert"));
        assertThat(config.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG).toString(), is("user-key"));
    }

    @Test
    public void testSaslPlaintextConnectionWithProjectedServiceAccountToken() {
        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(null, new ProjectedServiceAccountAuthIdentity(SA_TOKEN_PATH), new Properties());

        assertThat(config.size(), is(8));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SASL_PLAINTEXT"));
        assertThat(config.get(SaslConfigs.SASL_MECHANISM), is("OAUTHBEARER"));
        assertThat(config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS), is("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
        assertThat(config.get(SaslConfigs.SASL_JAAS_CONFIG), is("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.access.token.location=\"" + SA_TOKEN_PATH + "\";"));
    }

    @Test
    public void testSaslSslConnectionWithProjectedServiceAccountToken() {
        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(mockPemTrustSet(), new ProjectedServiceAccountAuthIdentity(SA_TOKEN_PATH), new Properties());

        assertThat(config.size(), is(10));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SASL_SSL"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), is("PEM"));
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca1")); // The order is not deterministic. So we check both certificates are present
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString(), containsString("ca2"));
        assertThat(config.get(SaslConfigs.SASL_MECHANISM), is("OAUTHBEARER"));
        assertThat(config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS), is("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
        assertThat(config.get(SaslConfigs.SASL_JAAS_CONFIG), is("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.access.token.location=\"" + SA_TOKEN_PATH + "\";"));
    }

    @Test
    public void testSaslSslWithPublicCAConnectionWithProjectedServiceAccountToken() {
        Properties customConfig = new Properties();
        customConfig.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");

        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        Properties config = defaultAdminClientProvider.adminClientConfiguration(null, new ProjectedServiceAccountAuthIdentity(SA_TOKEN_PATH), customConfig);

        assertThat(config.size(), is(8));
        assertDefaultConfigs(config);
        assertThat(config.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SASL_SSL"));
        assertThat(config.get(SaslConfigs.SASL_MECHANISM), is("OAUTHBEARER"));
        assertThat(config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS), is("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
        assertThat(config.get(SaslConfigs.SASL_JAAS_CONFIG), is("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.access.token.location=\"" + SA_TOKEN_PATH + "\";"));
    }

    @Test
    public void testNullConfig() {
        DefaultAdminClientProvider defaultAdminClientProvider = new DefaultAdminClientProvider();
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> defaultAdminClientProvider.adminClientConfiguration(null, mockPemAuthIdentity(), null));
        assertThat(ex.getMessage(), is("The config parameter should not be null"));
    }

    @Test
    public void tesCreateControllerAdminClientConfig() {
        DefaultAdminClientProvider defaultAdminClientProvider = spy(DefaultAdminClientProvider.class);
        // We expect a failure from creating an actual admin client since the bootstrap is not real
        assertThrows(RuntimeException.class, () -> defaultAdminClientProvider.createControllerAdminClient("my-kafka-controller:9090", null, null));

        ArgumentCaptor<Properties> configsCapture = ArgumentCaptor.forClass(Properties.class);
        verify(defaultAdminClientProvider).adminClientConfiguration(eq(null), eq(null), configsCapture.capture());
        Properties configs = configsCapture.getValue();

        assertThat(configs.size(), is(5));
        assertThat(configs.getProperty(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG), is("my-kafka-controller:9090"));
        assertDefaultConfigs(configs);
    }

    @Test
    public void testCreateBrokerAdminClient() {
        DefaultAdminClientProvider defaultAdminClientProvider = spy(DefaultAdminClientProvider.class);
        // We expect a failure from creating an actual admin client since the bootstrap is not real
        assertThrows(RuntimeException.class, () -> defaultAdminClientProvider.createAdminClient("my-kafka-broker:9092", null, null));

        ArgumentCaptor<Properties> configsCapture = ArgumentCaptor.forClass(Properties.class);
        verify(defaultAdminClientProvider).adminClientConfiguration(eq(null), eq(null), configsCapture.capture());
        Properties configs = configsCapture.getValue();

        assertThat(configs.size(), is(5));
        assertThat(configs.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), is("my-kafka-broker:9092"));
        assertDefaultConfigs(configs);
    }

    public static PemTrustSet mockPemTrustSet() {
        PemTrustSet mockTrustSet = mock(PemTrustSet.class);
        when(mockTrustSet.trustedCertificatesString()).thenReturn(String.format("%s%n%s", CA1, CA2));
        return mockTrustSet;
    }

    public static PemAuthIdentity mockPemAuthIdentity() {
        Secret secretWithCertificate = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName("test-cluster"))
                    .withNamespace("test-namespace")
                .endMetadata()
                .withData(Map.of("cluster-operator.key", Util.encodeToBase64(USER_KEY),
                        "cluster-operator.crt", Util.encodeToBase64(USER_CERT)))
                .build();
        return PemAuthIdentity.clusterOperator(secretWithCertificate);
    }
}
