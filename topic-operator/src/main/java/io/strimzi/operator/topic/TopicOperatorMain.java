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
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.http.HealthCheckAndMetricsServer;
import io.strimzi.operator.common.http.Liveness;
import io.strimzi.operator.common.http.Readiness;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import org.apache.kafka.clients.admin.Admin;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Entrypoint for unidirectional TO.
 */
public class TopicOperatorMain implements Liveness, Readiness {
    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorMain.class);
    private final static long INFORMER_PERIOD_MS = 2_000;
    
    private final ResourceEventHandler<KafkaTopic> handler;
    private final String namespace;
    private final KubernetesClient client;
    /* test */ final BatchingLoop queue;
    private final long resyncIntervalMs;
    private final BasicItemStore<KafkaTopic> itemStore;
    /* test */ final BatchingTopicController controller;
    private final Admin admin;
    private SharedIndexInformer<KafkaTopic> informer; // guarded by this
    Thread shutdownHook; // guarded by this

    private final HealthCheckAndMetricsServer healthAndMetricsServer;

    TopicOperatorMain(String namespace,
                      Map<String, String> selector,
                      Admin admin,
                      KubernetesClient client,
                      TopicOperatorConfig config) {
        Objects.requireNonNull(namespace);
        Objects.requireNonNull(selector);
        this.namespace = namespace;
        this.client = client;
        this.resyncIntervalMs = config.fullReconciliationIntervalMs();
        this.admin = admin;
        TopicOperatorMetricsProvider metricsProvider = createMetricsProvider();
        TopicOperatorMetricsHolder metrics = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, Labels.fromMap(selector), metricsProvider);
        this.controller = new BatchingTopicController(config, selector, admin, client, metrics, new ReplicasChangeHandler(config));
        this.itemStore = new BasicItemStore<>(Cache::metaNamespaceKeyFunc);
        this.queue = new BatchingLoop(config.maxQueueSize(), controller, 1, config.maxBatchSize(), config.maxBatchLingerMs(), itemStore, this::stop, metrics, namespace);
        this.handler = new TopicOperatorEventHandler(config, queue, metrics);
        this.healthAndMetricsServer = new HealthCheckAndMetricsServer(8080, this, this, metricsProvider);
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
        informer = Crds.topicOperation(client)
                .inNamespace(namespace)
                // Do NOT use withLabels to filter the informer, since the controller is stateful
                // (topics need to be added to removed from TopicController.topics if KafkaTopics transition between
                // selected and unselected).
                .runnableInformer(INFORMER_PERIOD_MS)
                // The informer interval acts like a heartbeat, then each handler interval will cause a resync at 
                // some interval of the overall heartbeat. The closer these values are together the more likely it 
                // is that the handler skips one informer intervals. Setting both intervals to the same value generates 
                // just enough skew that when the informer checks if the handler is ready for resync it sees that 
                // it still needs another couple of micro-seconds and skips to the next informer level resync.
                .addEventHandlerWithResyncPeriod(handler, resyncIntervalMs + INFORMER_PERIOD_MS)
                .itemStore(itemStore);
        LOGGER.infoOp("Starting informer");
        informer.run();
        LOGGER.infoOp("TopicOperator started");
    }

    synchronized void stop() {
        if (shutdownHook == null) {
            throw new IllegalStateException();
        }
        // shutdown(), will be be invoked indirectly by calling
        // h.run() has the side effect of nullifying this.shutdownHook
        // so retain a reference now so we have something to call
        // removeShutdownHook() with.
        var h = shutdownHook;
        // Call run (not start()) on the thread so that shutdown() is executed
        // on this thread.
        shutdown();
        // stop() is _not_ called from the shutdown hook, so calling
        // removeShutdownHook() should not cause IAE.
        Runtime.getRuntime().removeShutdownHook(h);
    }

    private synchronized void shutdown() {
        // Note: This method can be invoked on either via the shutdown hook thread or
        // on the thread(s) on which stop()/start() are called
        LOGGER.infoOp("Shutdown initiated");
        try {
            shutdownHook = null;
            if (informer != null) {
                informer.stop();
                informer = null;
            }
            this.queue.stop();
            this.admin.close();
            this.healthAndMetricsServer.stop();
            LOGGER.infoOp("Shutdown completed normally");
        } catch (InterruptedException e) {
            LOGGER.infoOp("Interrupted during shutdown");
            throw new RuntimeException(e);
        }
    }

    /**
     * Entrypoint for unidirectional TO.
     * @param args Command line args
     * @throws Exception If bad things happen
     */
    public static void main(String[] args) throws Exception {
        TopicOperatorConfig topicOperatorConfig = TopicOperatorConfig.buildFromMap(System.getenv());
        TopicOperatorMain operator = operator(topicOperatorConfig, kubeClient(), Admin.create(topicOperatorConfig.adminClientConfig()));
        operator.start();
    }

    static TopicOperatorMain operator(TopicOperatorConfig topicOperatorConfig, KubernetesClient client, Admin admin) throws ExecutionException, InterruptedException {
        return new TopicOperatorMain(topicOperatorConfig.namespace(), topicOperatorConfig.labelSelector().toMap(), admin, client, topicOperatorConfig);
    }

    static KubernetesClient kubeClient() {
        return new OperatorKubernetesClientBuilder(
                    "strimzi-topic-operator",
                    TopicOperatorMain.class.getPackage().getImplementationVersion())
                .build();
    }

    @Override
    public boolean isAlive() {
        boolean running;
        synchronized (this) {
            running = informer.isRunning();
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
            running = informer.isRunning();
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
     * @return MetricsProvider instance
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
