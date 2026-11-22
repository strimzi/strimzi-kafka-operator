/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.extrametrics.ExtraMetricsResources;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.MtlsAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.ServiceAccountAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This class is used to generate the Kafka Admin client configuration used by Extra Metrics to connect to the Kafka
 * cluster. The configuration is passed to Extra Metrics as is in an environment variable. The certificates are not
 * part of the configuration. They are loaded by the Kubernetes Secret config provider. This class is using the builder
 * pattern to make it easy to test the different parts. To generate the configuration, it is using the PrintWriter.
 */
public class ExtraMetricsConfigurationBuilder {
    // the secrets file template: <namespace>/<secret_name>:<secret_key>
    private static final String PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER = "${strimzisecrets:%s/%s:%s}";

    private final String namespace;
    private final String clusterName;
    private final StringWriter stringWriter = new StringWriter();
    private final PrintWriter writer = new PrintWriter(stringWriter);

    /**
     * Extra Metrics configuration builder constructor. It configures the bootstrap servers pointing to the replication
     * listener of the Kafka cluster.
     *
     * @param namespace     Namespace of the Kafka cluster
     * @param clusterName   Name of the Kafka cluster
     */
    public ExtraMetricsConfigurationBuilder(String namespace, String clusterName) {
        this.namespace = namespace;
        this.clusterName = clusterName;

        writer.println("bootstrap.servers=" + KafkaResources.bootstrapServiceName(clusterName) + ":" + KafkaCluster.REPLICATION_PORT);
    }

    /**
     * Configures the security protocol, encryption, and authentication based on the security of the Kafka cluster's
     * internal communication. When TLS encryption is used, it also configures the Kubernetes Secret config provider
     * used to load the certificates from the Secrets.
     *
     * @param securityContext   Security context of the Kafka cluster
     *
     * @return  Returns the builder instance
     */
    @SuppressWarnings("checkstyle:NoFullyQualifiedClassNames") // False positive, fully qualified class names used in strings
    public ExtraMetricsConfigurationBuilder withSecurity(KafkaClusterSecurityContext securityContext) {
        boolean tls = securityContext.encryption() instanceof TlsEncryptionConfiguration;
        boolean sasl = securityContext.authentication() instanceof ServiceAccountAuthenticationConfiguration;

        writer.println("security.protocol=" + (sasl ? "SASL_" : "") + (tls ? "SSL" : "PLAINTEXT"));

        if (tls) {
            writer.println("ssl.truststore.type=PEM");
            writer.println("ssl.truststore.certificates=" + secretPlaceholder(KafkaResources.trustBundleSecretName(clusterName), "cluster-ca.crt"));
        }

        if (securityContext.authentication() instanceof MtlsAuthenticationConfiguration) {
            // mTLS can be used only with TLS encryption
            writer.println("ssl.keystore.type=PEM");
            writer.println("ssl.keystore.certificate.chain=" + secretPlaceholder(ExtraMetricsResources.secretName(clusterName), "extra-metrics.crt"));
            writer.println("ssl.keystore.key=" + secretPlaceholder(ExtraMetricsResources.secretName(clusterName), "extra-metrics.key"));
        } else if (sasl) {
            writer.println("sasl.mechanism=OAUTHBEARER");
            writer.println("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            writer.println("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location=\"/var/run/secrets/strimzi.io/token\";");
        }

        if (tls) {
            // The config provider is needed only when some values are loaded from Secrets
            writer.println("config.providers=strimzisecrets");
            writer.println("config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider");
        }

        return this;
    }

    /**
     * Configures the user-provided Admin client options. They have to be added last. Forbidden options (connection,
     * security, and config providers) are already filtered out, and getConfiguration() escapes keys and values in the
     * properties format, so a user value cannot inject additional lines that would override the generated options.
     *
     * @param userConfiguration     User-provided Admin client configuration or null if not configured
     *
     * @return  Returns the builder instance
     */
    public ExtraMetricsConfigurationBuilder withUserConfiguration(ExtraMetricsAdminClientConfiguration userConfiguration) {
        if (userConfiguration != null) {
            writer.print(userConfiguration.getConfiguration());
        }

        return this;
    }

    /**
     * Generates the Kafka Admin client configuration in the Java properties format.
     *
     * @return  Kafka Admin client configuration as a multi-line string with key=value pairs
     */
    public String build() {
        writer.flush();
        return stringWriter.toString();
    }

    private String secretPlaceholder(String secretName, String key) {
        return String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, secretName, key);
    }
}
