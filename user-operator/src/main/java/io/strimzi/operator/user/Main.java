/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.http.HealthCheckAndMetricsServer;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.operator.user.operator.DisabledSimpleAclOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.operator.user.operator.QuotasOperator;
import io.strimzi.operator.user.operator.ScramCredentialsOperator;
import io.strimzi.operator.user.operator.SimpleAclOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The main class of the Strimzi User Operator
 *
 * Due to the number of classes instantiated in Main for bootstrapping the
 * operator, the checkstyle error for Class Data Abstraction Coupling is
 * disabled here. See
 * https://checkstyle.sourceforge.io/checks/metrics/classdataabstractioncoupling.html
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    /**
     * Main method which starts the webserver with healthchecks and metrics and the UserController which is responsible
     * for handling users
     *
     * @param args  Startup arguments
     */
    public static void main(String[] args) {
        LOGGER.info("UserOperator {} is starting", Main.class.getPackage().getImplementationVersion());

        // Log environment information
        Util.printEnvInfo();

        // Disable DNS caching
        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        // Create and log UserOperatorConfig
        UserOperatorConfig config = UserOperatorConfig.buildFromMap(System.getenv());
        LOGGER.info("UserOperator configuration is {}", config);

        // Create KubernetesClient, AdminClient and KafkaUserOperator classes
        ExecutorService kafkaUserOperatorExecutor = Executors.newFixedThreadPool(config.getUserOperationsThreadPoolSize(), new OperatorWorkThreadFactory());
        KubernetesClient client = new OperatorKubernetesClientBuilder("strimzi-user-operator", Main.class.getPackage().getImplementationVersion()).build();
        SecretOperator secretOperator = new SecretOperator(kafkaUserOperatorExecutor, client);
        Admin adminClient = createAdminClient(config, secretOperator, new DefaultAdminClientProvider());
        var kafkaUserCrdOperator = new CrdOperator<>(kafkaUserOperatorExecutor, client, KafkaUser.class, KafkaUserList.class, "KafkaUser");

        KafkaUserOperator kafkaUserOperator = new KafkaUserOperator(
                config,
                new OpenSslCertManager(),
                secretOperator,
                kafkaUserCrdOperator,
                new ScramCredentialsOperator(adminClient, config, kafkaUserOperatorExecutor),
                new QuotasOperator(adminClient, config, kafkaUserOperatorExecutor),
                config.isAclsAdminApiSupported() ? new SimpleAclOperator(adminClient, config, kafkaUserOperatorExecutor) : new DisabledSimpleAclOperator()
        );

        MetricsProvider metricsProvider = createMetricsProvider();

        // Create the User controller
        UserController controller = new UserController(
                config,
                secretOperator,
                kafkaUserCrdOperator,
                kafkaUserOperator,
                metricsProvider
        );

        // Create the health check and metrics server
        HealthCheckAndMetricsServer healthCheckAndMetricsServer = new HealthCheckAndMetricsServer(controller, controller, metricsProvider);

        // Start health check server, KafkaUser operator and the controller
        healthCheckAndMetricsServer.start();
        kafkaUserOperator.start();
        controller.start();

        // Register shutdown hooks
        LOGGER.info("Registering shutdown hook");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Requesting controller to stop");
            controller.stop();

            LOGGER.info("Requesting KafkaUser operator to stop");
            kafkaUserOperator.stop();
            kafkaUserOperatorExecutor.shutdownNow(); // We do not wait for termination

            LOGGER.info("Requesting controller to stop");
            healthCheckAndMetricsServer.stop();

            LOGGER.info("Requesting Kafka Admin client to stop");
            adminClient.close();

            LOGGER.info("Requesting Kubernetes client to stop");
            client.close();

            LOGGER.info("Shutdown complete");
        }));
    }

    /**
     * Creates the Kafka Admin API client
     *
     * @param config                User Operator configuration
     * @param secretOperator        Secret operator for managing secrets
     * @param adminClientProvider   Admin client provider
     *
     * @return  An instance of the Admin API client
     */
    private static Admin createAdminClient(UserOperatorConfig config, SecretOperator secretOperator, AdminClientProvider adminClientProvider)    {
        PemTrustSet pemTrustSet = new PemTrustSet(getSecret(secretOperator, config.getCaNamespaceOrNamespace(), config.getClusterCaCertSecretName()));
        Secret uoKeyAndCert = getSecret(secretOperator, config.getCaNamespaceOrNamespace(), config.getEuoKeySecretName());
        // When the UO secret is not null (i.e. mTLS is used), we create a PemAuthIdentity. Otherwise, we just pass null.
        PemAuthIdentity pemAuthIdentity = uoKeyAndCert != null ? PemAuthIdentity.entityOperator(uoKeyAndCert) : null;

        return adminClientProvider.createAdminClient(
                config.getKafkaBootstrapServers(),
                pemTrustSet,
                pemAuthIdentity,
                config.getKafkaAdminClientConfiguration());
    }

    /**
     * Fetch a secret with the given name and namespace using the secretOperator if
     * the name is present.
     *
     * @param secretOperator secret operator to retrieve the secret
     * @param namespace      namespace of the secret
     * @param name           name of the secret
     * @return the secret or null if not found or the name is not given
     */
    private static Secret getSecret(SecretOperator secretOperator, String namespace, String name) {
        if (name != null && !name.isEmpty()) {
            return secretOperator.get(namespace, name);
        }
        return null;
    }

    /**
     * Creates the MetricsProvider instance based on a PrometheusMeterRegistry and binds the JVM metrics to it
     *
     * @return  MetricsProvider instance
     */
    private static MetricsProvider createMetricsProvider()  {
        MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        // Bind JVM metrics
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);

        return new MicrometerMetricsProvider(registry);
    }

    private static class OperatorWorkThreadFactory implements ThreadFactory {
        private final AtomicInteger threadCounter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "operator-thread-pool-" + threadCounter.getAndIncrement());
        }
    }
}
