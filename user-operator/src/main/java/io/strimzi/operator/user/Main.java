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
import io.strimzi.api.kafka.Crds;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.user.operator.DisabledScramCredentialsOperator;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The main class of the Strimzi User Operator
 */
public class Main {
    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    // Registers the CRDs to be deserialized automatically
    static {
        try {
            Crds.registerCustomKinds();
        } catch (Error | RuntimeException t) {
            LOGGER.error("Failed to register CRDs", t);
            throw t;
        }
    }

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
        UserOperatorConfig config = UserOperatorConfig.fromMap(System.getenv());
        LOGGER.info("Cluster Operator configuration is {}", config);

        // Create KubernetesClient, AdminClient and KafkaUserOperator classes
        KubernetesClient client = new OperatorKubernetesClientBuilder("strimzi-user-operator", Main.class.getPackage().getImplementationVersion()).build();
        Admin adminClient = createAdminClient(config, client, new DefaultAdminClientProvider());
        AtomicInteger kafkaUserOperatorExecutorThreadCounter = new AtomicInteger(0);
        ExecutorService kafkaUserOperatorExecutor = Executors.newFixedThreadPool(config.getUserOperationsThreadPoolSize(), r -> new Thread(r, "operator-thread-pool-" + kafkaUserOperatorExecutorThreadCounter.getAndIncrement()));
        KafkaUserOperator kafkaUserOperator = new KafkaUserOperator(
                config,
                client,
                new OpenSslCertManager(),
                config.isKraftEnabled() ? new DisabledScramCredentialsOperator() : new ScramCredentialsOperator(adminClient, config, kafkaUserOperatorExecutor),
                new QuotasOperator(adminClient, config, kafkaUserOperatorExecutor),
                config.isAclsAdminApiSupported() ? new SimpleAclOperator(adminClient, config, kafkaUserOperatorExecutor) : new DisabledSimpleAclOperator(),
                kafkaUserOperatorExecutor
        );

        MetricsProvider metricsProvider = createMetricsProvider();

        // Create the User controller
        UserController controller = new UserController(
                config,
                client,
                kafkaUserOperator,
                metricsProvider
        );

        // Create the health check and metrics server
        HealthCheckAndMetricsServer healthCheckAndMetricsServer = new HealthCheckAndMetricsServer(controller, metricsProvider);

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
     * @param client                Kubernetes client
     * @param adminClientProvider   Admin client provider
     *
     * @return  An instance of the Admin API client
     */
    private static Admin createAdminClient(UserOperatorConfig config, KubernetesClient client, AdminClientProvider adminClientProvider)    {
        Secret clusterCaCert = null;
        if (config.getClusterCaCertSecretName() != null && !config.getClusterCaCertSecretName().isEmpty()) {
            clusterCaCert = client.secrets().inNamespace(config.getCaNamespace()).withName(config.getClusterCaCertSecretName()).get();
        }

        Secret uoKeyAndCert = null;
        if (config.getEuoKeySecretName() != null && !config.getEuoKeySecretName().isEmpty()) {
            uoKeyAndCert = client.secrets().inNamespace(config.getCaNamespace()).withName(config.getEuoKeySecretName()).get();
        }

        return adminClientProvider.createAdminClient(
                config.getKafkaBootstrapServers(),
                clusterCaCert,
                uoKeyAndCert,
                uoKeyAndCert != null ? "entity-operator" : null, // When the UO secret is not null (i.e. mTLS is used), we set the name. Otherwise, we just pass null.
                config.getKafkaAdminClientConfiguration());
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
}
