/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The main class of the Strimzi Topic Operator.
 */
public class Main {
    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    private Main() { }

    /**
     * The main method that starts the Topic Operator which is responsible for managing topics
     *
     * @param args  Startup arguments
     */
    public static void main(String[] args) {
        LOGGER.info("TopicOperator {} is starting", Main.class.getPackage().getImplementationVersion());

        // Log environment information
        Util.printEnvInfo();

        // Disable DNS caching
        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        // Create and log configuration
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(System.getenv());
        LOGGER.info("TopicOperator configuration is {}", config);

        // Create KubernetesClient, AdminClient and TopicOperator classes
        ExecutorService topicOperatorExecutor = Executors.newFixedThreadPool(config.topicOperationsThreadPoolSize(), new OperatorWorkThreadFactory());
        KubernetesClient client = new OperatorKubernetesClientBuilder("strimzi-topic-operator", Main.class.getPackage().getImplementationVersion()).build();
        SecretOperator secretOperator = new SecretOperator(topicOperatorExecutor, client);
        Admin kafkaAdmin = createAdminClient(config, secretOperator, new DefaultAdminClientProvider());

        TopicOperator operator = new TopicOperator(config, client, kafkaAdmin);
        operator.start();
    }

    /**
     * Creates the Kafka Admin API client
     *
     * @param config                Topic Operator configuration
     * @param secretOperator        Secret operator for managing secrets
     * @param adminClientProvider   Admin client provider
     *
     * @return  An instance of the Admin API client
     */
    /* test */ static Admin createAdminClient(TopicOperatorConfig config, SecretOperator secretOperator, AdminClientProvider adminClientProvider)    {
        Secret clusterCaCert = getSecret(secretOperator, config.clusterNamespace(), config.tlsTrustedCertsSecretName());
        // When the cluster CA secret is not null (i.e. TLS is used), we create a PemTrustSet. Otherwise, we just pass null.
        PemTrustSet pemTrustSet = clusterCaCert != null ? new PemTrustSet(clusterCaCert) : null;

        Secret toKeyAndCert = getSecret(secretOperator, config.clusterNamespace(), config.tlsSecretName());
        // When the TO secret is not null (i.e. mTLS is used), we create a PemAuthIdentity. Otherwise, we just pass null.
        PemAuthIdentity pemAuthIdentity = toKeyAndCert != null ? PemAuthIdentity.entityOperator(toKeyAndCert, config.tlsKeyName(), config.tlsCertName()) : null;

        return adminClientProvider.createAdminClient(
                config.bootstrapServers(),
                pemTrustSet,
                pemAuthIdentity,
                config.adminClientConfig());
    }

    /**
     * Fetch a secret with the given name and namespace using the secretOperator if the name is present.
     *
     * @param secretOperator Secret operator to retrieve the secret
     * @param namespace      Namespace of the secret
     * @param name           Name of the secret
     *
     * @return The secret
     *
     * @throws RuntimeException if the Secret name is not null and it is not found
     */
    private static Secret getSecret(SecretOperator secretOperator, String namespace, String name) {
        if (name != null && !name.isEmpty()) {
            Secret secret = secretOperator.get(namespace, name);
            if (secret != null) {
                return secret;
            } else {
                throw new RuntimeException("Secret " + name + " in namespace " + namespace + " with certificates was configured but is missing.");
            }
        } else {
            return null;
        }
    }

    private static class OperatorWorkThreadFactory implements ThreadFactory {
        private final AtomicInteger threadCounter = new AtomicInteger(0);

        private OperatorWorkThreadFactory() { }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "operator-thread-pool-" + threadCounter.getAndIncrement());
        }
    }
}
