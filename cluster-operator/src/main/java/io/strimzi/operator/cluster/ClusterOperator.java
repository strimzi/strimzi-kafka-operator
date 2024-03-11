/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.cluster.operator.assembly.AbstractOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMaker2AssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.ReconnectingWatcher;
import io.strimzi.operator.cluster.operator.assembly.StrimziPodSetController;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

/**
 * An "operator" for managing assemblies of various types <em>in a particular namespace</em>.
 * The Cluster Operator's multiple namespace support is achieved by deploying multiple
 * {@link ClusterOperator}'s in Vertx.
 */
public class ClusterOperator extends AbstractVerticle {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperator.class.getName());

    private final String namespace;
    private final ClusterOperatorConfig config;

    private final Map<String, ReconnectingWatcher<?>> watchByKind = new ConcurrentHashMap<>();

    private long reconcileTimer;
    private final KafkaAssemblyOperator kafkaAssemblyOperator;
    private final KafkaConnectAssemblyOperator kafkaConnectAssemblyOperator;
    private final KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator;
    private final KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator;
    private final KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator;
    private final KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator;
    private final ResourceOperatorSupplier resourceOperatorSupplier;

    private StrimziPodSetController strimziPodSetController;

    // this field is required to keep the underlying shared worker pool alive
    @SuppressWarnings("unused")
    private WorkerExecutor sharedWorkerExecutor;

    /**
     * Constructor
     *
     * @param namespace                         Namespace which this operator instance manages
     * @param config                            Cluster Operator configuration
     * @param kafkaAssemblyOperator             Kafka operator
     * @param kafkaConnectAssemblyOperator      KafkaConnect operator
     * @param kafkaMirrorMakerAssemblyOperator  KafkaMirrorMaker operator
     * @param kafkaMirrorMaker2AssemblyOperator KafkaMirrorMaker2 operator
     * @param kafkaBridgeAssemblyOperator       KafkaBridge operator
     * @param kafkaRebalanceAssemblyOperator    KafkaRebalance operator
     * @param resourceOperatorSupplier          Resource operator supplier
     */
    public ClusterOperator(String namespace,
                           ClusterOperatorConfig config,
                           KafkaAssemblyOperator kafkaAssemblyOperator,
                           KafkaConnectAssemblyOperator kafkaConnectAssemblyOperator,
                           KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator,
                           KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator,
                           KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator,
                           KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator,
                           ResourceOperatorSupplier resourceOperatorSupplier) {
        LOGGER.info("Creating ClusterOperator for namespace {}", namespace);
        this.namespace = namespace;
        this.config = config;
        this.kafkaAssemblyOperator = kafkaAssemblyOperator;
        this.kafkaConnectAssemblyOperator = kafkaConnectAssemblyOperator;
        this.kafkaMirrorMakerAssemblyOperator = kafkaMirrorMakerAssemblyOperator;
        this.kafkaMirrorMaker2AssemblyOperator = kafkaMirrorMaker2AssemblyOperator;
        this.kafkaBridgeAssemblyOperator = kafkaBridgeAssemblyOperator;
        this.kafkaRebalanceAssemblyOperator = kafkaRebalanceAssemblyOperator;
        this.resourceOperatorSupplier = resourceOperatorSupplier;
    }

    @Override
    public void start(Promise<Void> start) {
        LOGGER.info("Starting ClusterOperator for namespace {}", namespace);

        // Configure the executor here, but it is used only in other places
        sharedWorkerExecutor = getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", config.getOperationsThreadPoolSize(), TimeUnit.SECONDS.toNanos(120));

        List<Future<?>> startFutures = new ArrayList<>(8);
        startFutures.add(maybeStartStrimziPodSetController());

        if (!config.isPodSetReconciliationOnly()) {
            List<AbstractOperator<?, ?, ?, ?>> operators = new ArrayList<>(asList(
                    kafkaAssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaConnectAssemblyOperator,
                    kafkaBridgeAssemblyOperator, kafkaMirrorMaker2AssemblyOperator, kafkaRebalanceAssemblyOperator));
            for (AbstractOperator<?, ?, ?, ?> operator : operators) {
                startFutures.add(operator.createWatch(namespace).compose(w -> {
                    LOGGER.info("Opened watch for {} operator", operator.kind());
                    watchByKind.put(operator.kind(), w);
                    return Future.succeededFuture();
                }));
            }

            // Start the NodePool watch
            startFutures.add(kafkaAssemblyOperator.createNodePoolWatch(namespace).compose(w -> {
                LOGGER.info("Opened watch for {} operator", KafkaNodePool.RESOURCE_KIND);
                watchByKind.put(KafkaNodePool.RESOURCE_KIND, w);
                return Future.succeededFuture();
            }));

            // Start connector watch and add it to the map as well
            startFutures.add(kafkaConnectAssemblyOperator.createConnectorWatch(namespace).compose(w -> {
                LOGGER.info("Opened watch for {} operator", KafkaConnector.RESOURCE_KIND);
                watchByKind.put(KafkaConnector.RESOURCE_KIND, w);
                return Future.succeededFuture();
            }));
        }

        Future.join(startFutures)
                .compose(f -> {
                    LOGGER.info("Setting up periodic reconciliation for namespace {}", namespace);
                    this.reconcileTimer = vertx.setPeriodic(this.config.getReconciliationIntervalMs(), res2 -> {
                        if (!config.isPodSetReconciliationOnly()) {
                            LOGGER.info("Triggering periodic reconciliation for namespace {}", namespace);
                            reconcileAll("timer");
                        }
                    });

                    return Future.succeededFuture((Void) null);
                })
                .onComplete(start);
    }

    private Future<Void> maybeStartStrimziPodSetController() {
        return vertx.executeBlocking(() -> {
            try {
                strimziPodSetController = new StrimziPodSetController(
                        namespace,
                        config.getCustomResourceSelector(),
                        resourceOperatorSupplier.kafkaOperator,
                        resourceOperatorSupplier.connectOperator,
                        resourceOperatorSupplier.mirrorMaker2Operator,
                        resourceOperatorSupplier.strimziPodSetOperator,
                        resourceOperatorSupplier.podOperations,
                        resourceOperatorSupplier.metricsProvider,
                        config.getPodSetControllerWorkQueueSize()
                );
                strimziPodSetController.start();
                return null;
            } catch (Throwable e) {
                LOGGER.error("StrimziPodSetController start failed");
                throw e;
            }
        });
    }

    @Override
    public void stop(Promise<Void> stop) {
        LOGGER.info("Stopping ClusterOperator for namespace {}", namespace);
        vertx.cancelTimer(reconcileTimer);
        for (ReconnectingWatcher<?> watch : watchByKind.values()) {
            if (watch != null) {
                watch.close();
            }
        }

        strimziPodSetController.stop();
        stop.complete();
    }

    /**
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcileAll(String trigger) {
        if (!config.isPodSetReconciliationOnly()) {
            Handler<AsyncResult<Void>> ignore = ignored -> {
            };
            kafkaAssemblyOperator.reconcileAll(trigger, namespace, ignore);
            kafkaMirrorMakerAssemblyOperator.reconcileAll(trigger, namespace, ignore);
            kafkaConnectAssemblyOperator.reconcileAll(trigger, namespace, ignore);
            kafkaMirrorMaker2AssemblyOperator.reconcileAll(trigger, namespace, ignore);
            kafkaBridgeAssemblyOperator.reconcileAll(trigger, namespace, ignore);
            kafkaRebalanceAssemblyOperator.reconcileAll(trigger, namespace, ignore);
        }
    }
}
