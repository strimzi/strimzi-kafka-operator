/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractConnectOperator<C extends KubernetesClient, T extends CustomResource,
        L extends CustomResourceList<T>, D extends Doneable<T>, R extends Resource<T, D>>
        extends AbstractOperator<T, CrdOperator<C, T, L, D>> {

    private static final Logger log = LogManager.getLogger(AbstractConnectOperator.class.getName());
    public static final String STRIMZI_IO_USE_CONNECTOR_RESOURCES = "strimzi.io/use-connector-resources";

    private final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> connectorOperator;
    private final Function<Vertx, KafkaConnectApi> connectClientProvider;
    protected final ImagePullPolicy imagePullPolicy;
    protected final ConfigMapOperator configMapOperations;
    protected final ServiceOperator serviceOperations;
    protected final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    protected final List<LocalObjectReference> imagePullSecrets;
    protected final long operationTimeoutMs;
    protected final PlatformFeaturesAvailability pfa;
    protected final ServiceAccountOperator serviceAccountOperations;

    public AbstractConnectOperator(Vertx vertx, PlatformFeaturesAvailability pfa, String kind,
                                   CrdOperator<C, T, L, D> resourceOperator,
                                   ResourceOperatorSupplier supplier, ClusterOperatorConfig config,
                                   Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, kind, resourceOperator);
        this.connectorOperator = supplier.kafkaConnectorOperator;
        this.connectClientProvider = connectClientProvider;
        this.configMapOperations = supplier.configMapOperations;
        this.serviceOperations = supplier.serviceOperations;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.pfa = pfa;
    }

    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        // When deleting KafkaConnect we need to update the status of all selected KafkaConnector
        return connectorOperator.listAsync(reconciliation.namespace(), Labels.forCluster(reconciliation.name())).compose(connectors -> {
            List<Future> connectorFutures = new ArrayList<>();
            for (KafkaConnector connector : connectors) {
                connectorFutures.add(maybeUpdateConnectorStatus(reconciliation, connector,
                        noConnectCluster(reconciliation.namespace(), reconciliation.name())));
            }
            return CompositeFuture.join(connectorFutures);
        }).map(ignored -> Boolean.FALSE);
    }

    /**
     * Create a watch on {@code KafkaConnector} in the given {@code namespace}.
     * The watcher will:
     * <ul>
     * <li>{@link #reconcileConnectors(Reconciliation, CustomResource)} on the KafkaConnect or KafkaConnectS2I
     * identified by {@code KafkaConnector.metadata.labels[strimzi.io/cluster]}.</li>
     * <li>If there is a Connect and ConnectS2I cluster with the given name then the plain Connect one is used
     * (and an error is logged about the ambiguity).</li>
     * <li>The {@code KafkaConnector} status is updated with the result.</li>
     * </ul>
     * @param connectOperator The operator for {@code KafkaConnect}.
     * @param connectS2IOperator The operator for {@code KafkaConnectS2I}.
     * @param namespace The namespace.
     * @return A future which completes when the watch has been set up.
     */
    public static Future<Void> createConnectorWatch(AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> connectOperator,
            AbstractConnectOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> connectS2IOperator,
            String namespace) {
        // TODO Only create a single watch
        return Util.async(connectOperator.vertx, () -> {
            connectOperator.connectorOperator.watch(namespace, new Watcher<KafkaConnector>() {
                @Override
                public void eventReceived(Action action, KafkaConnector kafkaConnector) {
                    String connectName = kafkaConnector.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
                    Future<Void> f;
                    if (connectName != null) {
                        // Check whether a KafkaConnect/S2I exists
                        CompositeFuture.join(connectOperator.resourceOperator.getAsync(namespace, connectName),
                                             connectOperator.pfa.supportsS2I() ? connectS2IOperator.resourceOperator.getAsync(namespace, connectName) : Future.succeededFuture())
                                .compose(cf -> {
                                    KafkaConnect connect = cf.resultAt(0);
                                    KafkaConnectS2I connectS2i = cf.resultAt(1);
                                    KafkaConnectApi apiClient = connectOperator.connectClientProvider.apply(connectOperator.vertx);
                                    if (connect == null && connectS2i == null) {
                                        updateStatus(noConnectCluster(namespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                        return Future.succeededFuture();
                                    } else if (connect != null) {
                                        // grab the lock and call reconcileConnectors()
                                        // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                        Reconciliation reconciliation = new Reconciliation("connector-watch", connectOperator.kind(),
                                                kafkaConnector.getMetadata().getNamespace(), connectName);
                                        if (connectS2i != null) {
                                            log.warn("{}: There is both a KafkaConnect resource and a KafkaConnectS2I resource named {}. " +
                                                            "The KafkaConnect takes precedence for the connector {}",
                                                    reconciliation, connectName, connect.getMetadata().getName());
                                        }
                                        return connectOperator.withLock(reconciliation, LOCK_TIMEOUT_MS,
                                            () -> connectOperator.reconcileConnector(reconciliation,
                                                    KafkaConnectResources.serviceName(connectName), apiClient,
                                                    isUseResources(connect),
                                                    kafkaConnector.getMetadata().getName(), action == Action.DELETED ? null : kafkaConnector));
                                    } else {
                                        // grab the lock and call reconcileConnectors()
                                        // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                        Reconciliation r = new Reconciliation("connector-watch", connectS2IOperator.kind(),
                                                kafkaConnector.getMetadata().getNamespace(), connectName);
                                        return connectS2IOperator.withLock(r, LOCK_TIMEOUT_MS,
                                            () -> connectS2IOperator.reconcileConnector(r,
                                                    KafkaConnectResources.serviceName(connectName), apiClient,
                                                    isUseResources(connectS2i),
                                                    kafkaConnector.getMetadata().getName(), action == Action.DELETED ? null : kafkaConnector));
                                    }
                                }
                            );
                    } else {
                        updateStatus(new InvalidResourceException("Resource lacks label '"
                                        + Labels.STRIMZI_CLUSTER_LABEL
                                        + "': No connect cluster in which to create this connector."),
                                kafkaConnector, connectOperator.connectorOperator);
                    }
                }

                @Override
                public void onClose(KubernetesClientException e) {
                    if (e != null) {
                        throw e;
                    }
                }
            });
            return null;
        });
    }

    public static boolean isUseResources(HasMetadata connect) {
        return Annotations.booleanAnnotation(connect, STRIMZI_IO_USE_CONNECTOR_RESOURCES, false);
    }

    private static NoSuchResourceException noConnectCluster(String namespace, String connectName) {
        return new NoSuchResourceException(
                "KafkaConnect resource '" + connectName + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + namespace + ".");
    }

    /**
     * Reconcile all the connectors selected by the given connect instance, updated each connectors status with the result.
     * @param reconciliation The reconciliation
     * @param connect The connector
     * @return A future, failed if any of the connectors' statuses could not be updated.
     */
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, T connect) {
        String connectName = connect.getMetadata().getName();
        String namespace = connect.getMetadata().getNamespace();
        String host = KafkaConnectResources.serviceName(connectName);
        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);
        boolean useResources = isUseResources(connect);
        return CompositeFuture.join(apiClient.list(host, KafkaConnectCluster.REST_API_PORT),
                connectorOperator.listAsync(namespace,
                        Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build()))
        ).compose(cf -> {
            List<String> runningConnectorNames = cf.resultAt(0);
            List<KafkaConnector> desiredConnectors = cf.resultAt(1);
            log.debug("{}: {}} cluster: required connectors: {}", reconciliation, kind(), desiredConnectors);
            Set<String> deleteConnectorNames = new HashSet<>(runningConnectorNames);
            deleteConnectorNames.removeAll(desiredConnectors.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));
            log.debug("{}: {}} cluster: delete connectors: {}", reconciliation, kind(), deleteConnectorNames);
            Stream<Future<Void>> deletionFutures = deleteConnectorNames.stream().map(connectorName ->
                reconcileConnector(reconciliation, host, apiClient, useResources, connectorName, null)
            );
            Stream<Future<Void>> createUpdateFutures = desiredConnectors.stream()
                    .map(connector -> reconcileConnector(reconciliation, host, apiClient, useResources, connector.getMetadata().getName(), connector));
            return CompositeFuture.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        });
    }

    private Future<Void> reconcileConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, boolean useResources, String connectorName, KafkaConnector connector) {
        if (connector == null) {
            log.debug("{}: {}} cluster: deleting connector: {}", reconciliation, kind(), connectorName);
            return apiClient.delete(host, KafkaConnectCluster.REST_API_PORT, connectorName);
        } else {
            log.debug("{}: {}} cluster: creating/updating connector: {}", reconciliation, kind(), connectorName);
            if (connector.getSpec() == null) {
                return maybeUpdateConnectorStatus(reconciliation, connector,
                        new InvalidResourceException("spec property is required"));
            }
            if (!useResources) {
                return maybeUpdateConnectorStatus(reconciliation, connector,
                        new NoSuchResourceException("Not configured for connector management"));
            } else {
                return apiClient.createOrUpdatePutRequest(host, KafkaConnectCluster.REST_API_PORT,
                        connector.getMetadata().getName(), asJson(connector.getSpec()))
                        .compose(i -> maybeUpdateConnectorStatus(reconciliation, connector, null));
            }
        }
    }

    public static void updateStatus(Throwable error, KafkaConnector kafkaConnector2, CrdOperator<?, KafkaConnector, ?, ?> connectorOperations) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnector2, status, error);
        StatusDiff diff = new StatusDiff(kafkaConnector2.getStatus(), status);
        if (!diff.isEmpty()) {
            KafkaConnector copy = new KafkaConnectorBuilder(kafkaConnector2).build();
            copy.setStatus(status);
            connectorOperations.updateStatusAsync(copy);
        }
    }

    Future<Void> maybeUpdateConnectorStatus(Reconciliation reconciliation, KafkaConnector connector, Throwable error) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        if (error != null) {
            log.warn("{}: Error reconciling connector {}", reconciliation, connector.getMetadata().getName(), error);
        }
        StatusUtils.setStatusConditionAndObservedGeneration(connector, status, error != null ? Future.failedFuture(error) : Future.succeededFuture());
        return maybeUpdateStatusCommon(connectorOperator, connector, reconciliation, KafkaConnector::getStatus, status,
            (connector1, status1) -> {
                return new KafkaConnectorBuilder(connector1).withStatus(status1).build();
            });
    }

    private JsonObject asJson(KafkaConnectorSpec spec) {
        JsonObject connectorConfigJson = new JsonObject();
        if (spec.getConfig() != null) {
            for (Map.Entry<String, Object> cf : spec.getConfig().entrySet()) {
                String name = cf.getKey();
                if ("connector.class".equals(name)
                        || "tasks.max".equals(name)) {
                    // TODO include resource namespace and name in this message
                    log.warn("Configuration parameter {} in KafkaConnector.spec.config will be ignored and the value from KafkaConnector.spec will be used instead",
                            name);
                }
                connectorConfigJson.put(name, cf.getValue());
            }
        }
        return connectorConfigJson
                .put("connector.class", spec.getClassName())
                .put("tasks.max", spec.getTasksMax());
    }

    /**
     * Updates the Status field of the KafkaConnect CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param kafkaConnectAssembly The CR of KafkaConnect
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaConnectStatus which should be set
     *
     * @return
     */
    protected <T extends CustomResource, S extends Status, L extends CustomResourceList<T>, D extends Doneable<T>> Future<Void>
        maybeUpdateStatusCommon(CrdOperator<KubernetesClient, T, L, D> resourceOperator,
                                T kafkaConnectAssembly,
                                Reconciliation reconciliation,
                                Function<T, S> fn,
                                S desiredStatus,
                                BiFunction<T, S, T> copyWithStatus) {
        Future<Void> updateStatusFuture = Future.future();

        resourceOperator.getAsync(kafkaConnectAssembly.getMetadata().getNamespace(), kafkaConnectAssembly.getMetadata().getName()).setHandler(getRes -> {
            if (getRes.succeeded()) {
                T connect = getRes.result();

                if (connect != null) {
                    if (StatusUtils.isResourceV1alpha1(connect)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, connect.getApiVersion());
                        updateStatusFuture.complete();
                    } else {
                        S currentStatus = fn.apply(connect);

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            T resourceWithNewStatus = copyWithStatus.apply(connect, desiredStatus);

                            resourceOperator.updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
                                if (updateRes.succeeded()) {
                                    log.debug("{}: Completed status update", reconciliation);
                                    updateStatusFuture.complete();
                                } else {
                                    log.error("{}: Failed to update status", reconciliation, updateRes.cause());
                                    updateStatusFuture.fail(updateRes.cause());
                                }
                            });
                        } else {
                            log.debug("{}: Status did not change", reconciliation);
                            updateStatusFuture.complete();
                        }
                    }
                } else {
                    log.error("{}: Current Kafka Connect resource not found", reconciliation);
                    updateStatusFuture.fail("Current Kafka Connect resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka Connect resource and its status", reconciliation, getRes.cause());
                updateStatusFuture.fail(getRes.cause());
            }
        });

        return updateStatusFuture;
    }
}
