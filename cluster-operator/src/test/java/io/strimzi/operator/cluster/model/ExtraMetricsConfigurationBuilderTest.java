/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.extrametrics.ExtraMetricsResources;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.MtlsAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneEncryptionConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("checkstyle:NoFullyQualifiedClassNames") // Fully qualified class names are used in the expected config strings
public class ExtraMetricsConfigurationBuilderTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers=" + KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT;

    @Test
    public void testBootstrapServersOnly() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "")));
    }

    @Test
    public void testSecurityWithMtls() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), new MtlsAuthenticationConfiguration()))
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.certificates=" + placeholder(KafkaResources.trustBundleSecretName(CLUSTER_NAME), "cluster-ca.crt"),
                "ssl.keystore.type=PEM",
                "ssl.keystore.certificate.chain=" + placeholder(ExtraMetricsResources.secretName(CLUSTER_NAME), "extra-metrics.crt"),
                "ssl.keystore.key=" + placeholder(ExtraMetricsResources.secretName(CLUSTER_NAME), "extra-metrics.key"),
                "config.providers=strimzisecrets",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "")));
    }

    @Test
    public void testSecurityWithServiceAccountAuthentication() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), serviceAccountAuthentication()))
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=SASL_SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.certificates=" + placeholder(KafkaResources.trustBundleSecretName(CLUSTER_NAME), "cluster-ca.crt"),
                "sasl.mechanism=OAUTHBEARER",
                "sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location=\"/var/run/secrets/strimzi.io/token\";",
                "config.providers=strimzisecrets",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "")));
    }

    @Test
    public void testSecurityWithServiceAccountAuthenticationWithoutTls() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), serviceAccountAuthentication()))
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=OAUTHBEARER",
                "sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location=\"/var/run/secrets/strimzi.io/token\";",
                "")));
    }

    @Test
    public void testSecurityWithTlsOnly() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), new NoneAuthenticationConfiguration()))
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.certificates=" + placeholder(KafkaResources.trustBundleSecretName(CLUSTER_NAME), "cluster-ca.crt"),
                "config.providers=strimzisecrets",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "")));
    }

    @Test
    public void testSecurityWithoutTls() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration()))
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=PLAINTEXT",
                "")));
    }

    @Test
    public void testUserConfiguration() {
        ExtraMetricsAdminClientConfiguration userConfiguration = new ExtraMetricsAdminClientConfiguration(Reconciliation.DUMMY_RECONCILIATION,
                Map.<String, Object>of("default.api.timeout.ms", 10000).entrySet());

        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration()))
                .withUserConfiguration(userConfiguration)
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=PLAINTEXT",
                "default.api.timeout.ms=10000",
                "")));
    }

    @Test
    public void testNullUserConfiguration() {
        String configuration = new ExtraMetricsConfigurationBuilder(NAMESPACE, CLUSTER_NAME)
                .withSecurity(new KafkaClusterSecurityContext(new NoneEncryptionConfiguration(), new NoneAuthenticationConfiguration()))
                .withUserConfiguration(null)
                .build();

        assertThat(configuration, is(String.join("\n",
                BOOTSTRAP_SERVERS,
                "security.protocol=PLAINTEXT",
                "")));
    }

    private static String placeholder(String secretName, String key) {
        return "${strimzisecrets:" + NAMESPACE + "/" + secretName + ":" + key + "}";
    }

    private static AuthenticationConfiguration serviceAccountAuthentication() {
        return AuthenticationConfiguration.fromCrd(NAMESPACE, CLUSTER_NAME,
                new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build(), null);
    }
}
