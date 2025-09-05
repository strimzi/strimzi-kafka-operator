/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.BasicItemStore;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.http.HealthCheckAndMetricsServer;
import io.strimzi.operator.common.http.Liveness;
import io.strimzi.operator.common.http.Readiness;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlHandler;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import org.apache.kafka.clients.admin.Admin;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Topic Operator.
 */
public class TopicOperatorMain implements Liveness, Readiness {
    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorMain.class);
    private final static long INFORMER_RESYNC_CHECK_PERIOD_MS = 30_000;
    private final static int HEALTH_CHECK_PORT = 8080;

    private final TopicOperatorConfig config;
    private final KubernetesClient kubernetesClient;
    private final Admin kafkaAdminClient;
    private final CruiseControlClient cruiseControlClient;

    /* test */ final BatchingLoop queue;
    private final BasicItemStore<KafkaTopic> itemStore;
    /* test */ final BatchingTopicController controller;
    
    private SharedIndexInformer<KafkaTopic> informer; // guarded by this
    /* test */ volatile Thread shutdownHook;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    private final ResourceEventHandler<KafkaTopic> resourceEventHandler;
    private final HealthCheckAndMetricsServer healthAndMetricsServer;

    TopicOperatorMain(TopicOperatorConfig config) {
        this(config,
            new OperatorKubernetesClientBuilder(
                "strimzi-topic-operator",
                TopicOperatorMain.class.getPackage().getImplementationVersion()
            ).build(), 
            Admin.create(config.adminClientConfig()));
    }

    /* test */ TopicOperatorMain(TopicOperatorConfig config, KubernetesClient kubernetesClient, Admin kafkaAdminClient) {
        Objects.requireNonNull(config.namespace());
        Objects.requireNonNull(config.resourceLabels());
        this.config = config;
        var selector = config.resourceLabels().toMap();
        this.kubernetesClient = kubernetesClient;
        this.kafkaAdminClient = kafkaAdminClient;
        this.cruiseControlClient = TopicOperatorUtil.createCruiseControlClient(config);
        
        var metricsProvider = createMetricsProvider();
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, Labels.fromMap(selector), metricsProvider);
        var kubeHandler = new KubernetesHandler(config, metricsHolder, kubernetesClient);
        var kafkaHandler = new KafkaHandler(config, metricsHolder, kafkaAdminClient);
        var cruiseControlHandler = new CruiseControlHandler(config, metricsHolder, cruiseControlClient);

        this.controller = new BatchingTopicController(config, selector, kubeHandler, kafkaHandler, metricsHolder, cruiseControlHandler);
        this.itemStore = new BasicItemStore<>(Cache::metaNamespaceKeyFunc);
        this.queue = new BatchingLoop(config, controller, 1, itemStore, this::stop, metricsHolder);
        this.resourceEventHandler = new TopicEventHandler(config, queue, metricsHolder);
        this.healthAndMetricsServer = new HealthCheckAndMetricsServer(HEALTH_CHECK_PORT, this, this, metricsProvider);
    }

    synchronized void start() {
        LOGGER.infoOp("TopicOperator {} is starting", TopicOperatorMain.class.getPackage().getImplementationVersion());
        if (shutdownHook != null) {
            throw new IllegalStateException();
        }

        shutdownHook = new Thread(this::shutdown, "TopicOperator-shutdown-hook");
        LOGGER.infoOp("Installing shutdown hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        LOGGER.infoOp("Starting health and metrics");
        healthAndMetricsServer.start();
        LOGGER.infoOp("Starting queue");
        queue.start();
        informer = Crds.topicOperation(kubernetesClient)
                .inNamespace(config.namespace())
                // Do NOT use withLabels to filter the informer, since the controller is stateful
                // (topics need to be added to removed from TopicController.topics if KafkaTopics transition between
                // selected and unselected).
                .runnableInformer(INFORMER_RESYNC_CHECK_PERIOD_MS)
                // The informer resync check interval acts like a heartbeat, then each handler interval will cause a resync at
                // some interval of the overall heartbeat. The closer these values are together the more likely it 
                // is that the handler skips one informer intervals. Setting both intervals to the same value generates 
                // just enough skew that when the informer checks if the handler is ready for resync it sees that 
                // it still needs another couple of micro-seconds and skips to the next informer level resync.
                .addEventHandlerWithResyncPeriod(resourceEventHandler, config.fullReconciliationIntervalMs())
                .itemStore(itemStore);
        LOGGER.infoOp("Starting informer");
        informer.run();
        LOGGER.infoOp("TopicOperator started");
    }

    void stop() {
        // Make stop() safe if invoked multiple times or from different threads.
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        final Thread hook;
        synchronized (this) {
            // snapshot for removal after shutdown()
            hook = shutdownHook;
        }
        // Execute the actual shutdown sequence (idempotent).
        shutdown();

        // Try to detach the shutdown hook if we're not already in JVM shutdown.
        if (hook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down i.e., safe to ignore.
            }
        }
    }

    private synchronized void shutdown() {
        if (shutdownHook == null) {
            LOGGER.infoOp("Already shutting down");
            return; // already shut down
        }
        shutdownHook = null;
        // Note: This method can be invoked on either via the shutdown hook thread or
        // on the thread(s) on which stop()/start() are called
        LOGGER.infoOp("Shutdown initiated");

        try {
            // Idempotent resource teardown.
            if (informer != null) {
                informer.stop();
                informer = null;
            }
            this.queue.stop();
            this.kafkaAdminClient.close();
            this.kubernetesClient.close();
            this.cruiseControlClient.close();
            this.healthAndMetricsServer.stop();
            LOGGER.infoOp("Shutdown completed normally");
        } catch (InterruptedException e) {
            LOGGER.infoOp("Interrupted during shutdown");
            throw new RuntimeException(e);
        }
    }

    /**
     * @param args Command line args.
     */
    public static void main(String[] args) {
        var config = TopicOperatorConfig.buildFromMap(System.getenv());
        var operator = new TopicOperatorMain(config);
        operator.start();
    }

    @Override
    public boolean isAlive() {
        boolean running;
        synchronized (this) {
            running = informer != null && informer.isRunning();
        }
        if (!running) {
            LOGGER.infoOp("isAlive returning false because informer is not running");
            return false;
        } else {
            return queue.isAlive();
        }
    }

    @Override
    public boolean isReady() {
        boolean running;
        synchronized (this) {
            running = informer != null && informer.isRunning();
        }
        if (!running) {
            LOGGER.infoOp("isReady returning false because informer is not running");
            return false;
        } else {
            return queue.isReady();
        }
    }

    /**
     * Creates the MetricsProvider instance based on a PrometheusMeterRegistry
     * and binds the JVM metrics to it.
     *
     * @return MetricsProvider instance.
     */
    private static TopicOperatorMetricsProvider createMetricsProvider()  {
        MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        return new TopicOperatorMetricsProvider(registry);
    }
}
