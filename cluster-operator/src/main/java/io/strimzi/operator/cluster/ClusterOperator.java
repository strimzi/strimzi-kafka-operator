/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMaker2AssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.StrimziPodSetController;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
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

    private static final String NAME_SUFFIX = "-cluster-operator";
    private static final String CERTS_SUFFIX = NAME_SUFFIX + "-certs";

    private final KubernetesClient client;
    private final String namespace;
    private final ClusterOperatorConfig config;

    private final Map<String, Watch> watchByKind = new ConcurrentHashMap<>();

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
     * @param namespace                             Namespace which this operator instance manages
     * @param config                                Cluster Operator configuration
     * @param client                                Kubernetes client
     * @param kafkaAssemblyOperator                 Kafka operator
     * @param kafkaConnectAssemblyOperator          KafkaConnect operator
     * @param kafkaMirrorMakerAssemblyOperator      KafkaMirrorMaker operator
     * @param kafkaMirrorMaker2AssemblyOperator     KafkaMirrorMaker2 operator
     * @param kafkaBridgeAssemblyOperator           KafkaBridge operator
     * @param kafkaRebalanceAssemblyOperator        KafkaRebalance operator
     * @param resourceOperatorSupplier              Resource operator supplier
     */
    public ClusterOperator(String namespace,
                           ClusterOperatorConfig config,
                           KubernetesClient client,
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
        this.client = client;
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

        @SuppressWarnings({ "rawtypes" })
        List<Future> startFutures = new ArrayList<>(8);
        startFutures.add(maybeStartStrimziPodSetController());

        if (!config.isPodSetReconciliationOnly()) {
            List<AbstractOperator<?, ?, ?, ?>> operators = new ArrayList<>(asList(
                    kafkaAssemblyOperator, kafkaMirrorMakerAssemblyOperator,
                    kafkaConnectAssemblyOperator, kafkaBridgeAssemblyOperator, kafkaMirrorMaker2AssemblyOperator));
            for (AbstractOperator<?, ?, ?, ?> operator : operators) {
                startFutures.add(operator.createWatch(namespace, operator.recreateWatch(namespace)).compose(w -> {
                    LOGGER.info("Opened watch for {} operator", operator.kind());
                    watchByKind.put(operator.kind(), w);
                    return Future.succeededFuture();
                }));
            }

            startFutures.add(AbstractConnectOperator.createConnectorWatch(kafkaConnectAssemblyOperator, namespace, config.getCustomResourceSelector()));
            startFutures.add(kafkaRebalanceAssemblyOperator.createRebalanceWatch(namespace));
        }

        CompositeFuture.join(startFutures)
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
        Promise<Void> handler = Promise.promise();
        vertx.executeBlocking(future -> {
            try {
                if (config.featureGates().useStrimziPodSetsEnabled()) {
                    strimziPodSetController = new StrimziPodSetController(namespace, config.getCustomResourceSelector(), resourceOperatorSupplier.kafkaOperator,
                            resourceOperatorSupplier.strimziPodSetOperator, resourceOperatorSupplier.podOperations, resourceOperatorSupplier.metricsProvider, config.getPodSetControllerWorkQueueSize());
                    strimziPodSetController.start();
                }
                future.complete();
            } catch (Throwable e) {
                LOGGER.error("StrimziPodSetController start failed");
                future.fail(e);
            }
        }, handler);
        return handler.future();
    }

    @Override
    public void stop(Promise<Void> stop) {
        LOGGER.info("Stopping ClusterOperator for namespace {}", namespace);
        vertx.cancelTimer(reconcileTimer);
        for (Watch watch : watchByKind.values()) {
            if (watch != null) {
                watch.close();
            }
        }

        if (config.featureGates().useStrimziPodSetsEnabled()) {
            strimziPodSetController.stop();
        }

        client.close();
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

    /**
     * Name of the secret with the Cluster Operator certificates for connecting to the cluster
     *
     * @param cluster   Name of the Kafka cluster
     *
     * @return  Name of the Cluster Operator certificate secret
     */
    public static String secretName(String cluster) {
        return cluster + CERTS_SUFFIX;
    }
}
