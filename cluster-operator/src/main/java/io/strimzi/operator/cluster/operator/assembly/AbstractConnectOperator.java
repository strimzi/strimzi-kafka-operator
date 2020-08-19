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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ConnectTimeoutException;
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
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectS2IStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
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
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:CyclomaticComplexity"})
public abstract class AbstractConnectOperator<C extends KubernetesClient, T extends CustomResource,
        L extends CustomResourceList<T>, D extends Doneable<T>, R extends Resource<T, D>, S extends KafkaConnectStatus>
        extends AbstractOperator<T, CrdOperator<C, T, L, D>> {

    private static final Logger log = LogManager.getLogger(AbstractConnectOperator.class.getName());

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
    private final int port;

    private final Counter connectorsReconciliationsCounter;
    private final Counter connectorsFailedReconciliationsCounter;
    private final Counter connectorsSuccessfulReconciliationsCounter;
    private final AtomicInteger connectorsResourceCounter;
    private final Timer connectorsReconciliationsTimer;

    public AbstractConnectOperator(Vertx vertx, PlatformFeaturesAvailability pfa, String kind,
                                   CrdOperator<C, T, L, D> resourceOperator,
                                   ResourceOperatorSupplier supplier, ClusterOperatorConfig config,
                                   Function<Vertx, KafkaConnectApi> connectClientProvider,
                                   int port) {
        super(vertx, kind, resourceOperator, supplier.metricsProvider);
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
        this.port = port;

        // Setup metrics for connectors
        Tags metricTags = Tags.of(Tag.of("kind", KafkaConnector.RESOURCE_KIND));

        connectorsReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations",
                "Number of reconciliations done by the operator for individual resources",
                metricTags);

        connectorsFailedReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.failed",
                "Number of reconciliations done by the operator for individual resources which failed",
                metricTags);

        connectorsSuccessfulReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.successful",
                "Number of reconciliations done by the operator for individual resources which were successful",
                metricTags);

        connectorsResourceCounter = metrics.gauge(METRICS_PREFIX + "resources",
                "Number of custom resources the operator sees",
                metricTags);

        connectorsReconciliationsTimer = metrics.timer(METRICS_PREFIX + "reconciliations.duration",
                "The time the reconciliation takes to complete",
                metricTags);
    }

    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        // When deleting KafkaConnect we need to update the status of all selected KafkaConnector
        return connectorOperator.listAsync(reconciliation.namespace(), Labels.forStrimziCluster(reconciliation.name())).compose(connectors -> {
            List<Future> connectorFutures = new ArrayList<>();
            for (KafkaConnector connector : connectors) {
                connectorFutures.add(maybeUpdateConnectorStatus(reconciliation, connector, null,
                        noConnectCluster(reconciliation.namespace(), reconciliation.name())));
            }
            return CompositeFuture.join(connectorFutures);
        }).map(ignored -> Boolean.FALSE);
    }

    /**
     * Create a watch on {@code KafkaConnector} in the given {@code namespace}.
     * The watcher will:
     * <ul>
     * <li>{@link #reconcileConnectors(Reconciliation, CustomResource, KafkaConnectStatus, boolean, String)} on the KafkaConnect or KafkaConnectS2I
     * identified by {@code KafkaConnector.metadata.labels[strimzi.io/cluster]}.</li>
     * <li>If there is a Connect and ConnectS2I cluster with the given name then the plain Connect one is used
     * (and an error is logged about the ambiguity).</li>
     * <li>The {@code KafkaConnector} status is updated with the result.</li>
     * </ul>
     * @param connectOperator The operator for {@code KafkaConnect}.
     * @param connectS2IOperator The operator for {@code KafkaConnectS2I}.
     * @param watchNamespaceOrWildcard The namespace to watch.
     * @return A future which completes when the watch has been set up.
     */
    public static Future<Void> createConnectorWatch(AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>, KafkaConnectStatus> connectOperator,
            AbstractConnectOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>, KafkaConnectS2IStatus> connectS2IOperator,
            String watchNamespaceOrWildcard) {
        return Util.async(connectOperator.vertx, () -> {
            connectOperator.connectorOperator.watch(watchNamespaceOrWildcard, new Watcher<KafkaConnector>() {
                @Override
                public void eventReceived(Action action, KafkaConnector kafkaConnector) {
                    String connectorName = kafkaConnector.getMetadata().getName();
                    String connectorNamespace = kafkaConnector.getMetadata().getNamespace();
                    String connectorKind = kafkaConnector.getKind();
                    String connectName = kafkaConnector.getMetadata().getLabels() == null ? null : kafkaConnector.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
                    String connectNamespace = connectorNamespace;

                    switch (action) {
                        case ADDED:
                        case DELETED:
                        case MODIFIED:
                            Future<Void> f;
                            if (connectName != null) {
                                // Check whether a KafkaConnect/S2I exists
                                CompositeFuture.join(connectOperator.resourceOperator.getAsync(connectNamespace, connectName),
                                        connectOperator.pfa.supportsS2I() ?
                                                connectS2IOperator.resourceOperator.getAsync(connectNamespace, connectName) :
                                                Future.succeededFuture())
                                        .compose(cf -> {
                                            KafkaConnect connect = cf.resultAt(0);
                                            KafkaConnectS2I connectS2i = cf.resultAt(1);
                                            KafkaConnectApi apiClient = connectOperator.connectClientProvider.apply(connectOperator.vertx);
                                            if (connect == null && connectS2i == null) {
                                                log.info("{} {} in namespace {} was {}, but Connect cluster {} does not exist", connectorKind, connectorName, connectorNamespace, action, connectName);
                                                updateStatus(noConnectCluster(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                                return Future.succeededFuture();
                                            } else if (connect != null && isOlderOrAlone(connect.getMetadata().getCreationTimestamp(), connectS2i)) {
                                                // grab the lock and call reconcileConnectors()
                                                // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                                Reconciliation reconciliation = new Reconciliation("connector-watch", connectOperator.kind(),
                                                        kafkaConnector.getMetadata().getNamespace(), connectName);

                                                if (connect.getSpec() != null && connect.getSpec().getReplicas() == 0)  {
                                                    log.info("{}: {} {} in namespace {} was {}, but Connect cluster {} has 0 replicas", reconciliation, connectorKind, connectorName, connectorNamespace, action, connectName);
                                                    updateStatus(zeroReplicas(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                                    return Future.succeededFuture();
                                                } else {
                                                    log.info("{}: {} {} in namespace {} was {}", reconciliation, connectorKind, connectorName, connectorNamespace, action);

                                                    return connectOperator.withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                        () -> connectOperator.reconcileConnectorAndHandleResult(reconciliation,
                                                                    KafkaConnectResources.qualifiedServiceName(connectName, connectNamespace), apiClient,
                                                                    isUseResources(connect),
                                                                    kafkaConnector.getMetadata().getName(), action == Action.DELETED ? null : kafkaConnector)
                                                                    .compose(reconcileResult -> {
                                                                        log.info("{}: reconciled", reconciliation);
                                                                        return Future.succeededFuture(reconcileResult);
                                                                    }));
                                                }
                                            } else {
                                                // grab the lock and call reconcileConnectors()
                                                // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                                Reconciliation reconciliation = new Reconciliation("connector-watch", connectS2IOperator.kind(),
                                                        kafkaConnector.getMetadata().getNamespace(), connectName);

                                                if (connectS2i.getSpec() != null && connectS2i.getSpec().getReplicas() == 0)    {
                                                    log.info("{}: {} {} in namespace {} was {}, but Connect cluster {} has 0 replicas", reconciliation, connectorKind, connectorName, connectorNamespace, action, connectName);
                                                    updateStatus(zeroReplicas(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                                    return Future.succeededFuture();
                                                } else {
                                                    log.info("{}: {} {} in namespace {} was {}", reconciliation, connectorKind, connectorName, connectorNamespace, action);

                                                    return connectS2IOperator.withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                        () -> connectS2IOperator.reconcileConnectorAndHandleResult(reconciliation,
                                                                    KafkaConnectResources.qualifiedServiceName(connectName, connectNamespace), apiClient,
                                                                    isUseResources(connectS2i),
                                                                    kafkaConnector.getMetadata().getName(), action == Action.DELETED ? null : kafkaConnector)
                                                                    .compose(reconcileResult -> {
                                                                        log.info("{}: reconciled", reconciliation);
                                                                        return Future.succeededFuture(reconcileResult);
                                                                    }));
                                                }
                                            }
                                        });
                            } else {
                                updateStatus(new InvalidResourceException("Resource lacks label '"
                                                + Labels.STRIMZI_CLUSTER_LABEL
                                                + "': No connect cluster in which to create this connector."),
                                        kafkaConnector, connectOperator.connectorOperator);
                            }

                            break;
                        case ERROR:
                            log.error("Failed {} {} in namespace {} ", connectorKind, connectorName, connectorNamespace);
                            break;
                        default:
                            log.error("Unknown action: {} {} in namespace {}", connectorKind, connectorName, connectorNamespace);
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

    /**
     * Returns true if the resource is null or if the creationDate of the resource is newer than the creationDate. If
     * the dates are the same, it returns true. This is used to determine whether Connect and ConnectS2I both exist and
     * when yes which of them should get the connectors (the older one).
     *
     * @param creationDate  creation date of the initial resource
     * @param resource  resource to compare it with
     * @return
     */
    /*test*/ static boolean isOlderOrAlone(String creationDate, HasMetadata resource)  {
        return resource == null || creationDate.compareTo(resource.getMetadata().getCreationTimestamp()) <= 0;
    }

    public static boolean isUseResources(HasMetadata connect) {
        return Annotations.booleanAnnotation(connect, Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, false);
    }

    private static NoSuchResourceException noConnectCluster(String connectNamespace, String connectName) {
        return new NoSuchResourceException(
                "KafkaConnect resource '" + connectName + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + connectNamespace + ".");
    }

    private static RuntimeException zeroReplicas(String connectNamespace, String connectName) {
        return new RuntimeException(
                "Kafka Connect cluster '" + connectName + "' in namespace " + connectNamespace + " has 0 replicas.");
    }

    /**
     * Reconcile all the connectors selected by the given connect instance, updated each connectors status with the result.
     * @param reconciliation The reconciliation
     * @param connect The connector
     * @param connectStatus Status of the KafkaConnect or KafkaConnectS2I resource (will be used to set the available
     *                      connector plugins)
     * @param scaledToZero  Indicated whether the related Connect cluster is currently scaled to 0 replicas
     * @return A future, failed if any of the connectors' statuses could not be updated.
     */
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, T connect, S connectStatus, boolean scaledToZero, String desiredLogging) {
        String connectName = connect.getMetadata().getName();
        String namespace = connect.getMetadata().getNamespace();
        String host = KafkaConnectResources.qualifiedServiceName(connectName, namespace);

        if (!isUseResources(connect))    {
            return Future.succeededFuture();
        }

        if (scaledToZero)   {
            return connectorOperator.listAsync(namespace, Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build()))
                    .compose(connectors -> CompositeFuture.join(
                            connectors.stream().map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, zeroReplicas(namespace, connectName)))
                                    .collect(Collectors.toList())
                    ))
                    .map((Void) null);
        }

        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);

        return CompositeFuture.join(
                apiClient.list(host, port),
                connectorOperator.listAsync(namespace, Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())),
                apiClient.listConnectorPlugins(host, port),
                apiClient.updateConnectLoggers(host, port, desiredLogging)
        ).compose(cf -> {
            List<String> runningConnectorNames = cf.resultAt(0);
            List<KafkaConnector> desiredConnectors = cf.resultAt(1);
            List<ConnectorPlugin> connectorPlugins = cf.resultAt(2);

            log.debug("{}: Setting list of connector plugins in Kafka Connect status", reconciliation);
            connectStatus.setConnectorPlugins(connectorPlugins);

            if (connectorsResourceCounter != null)  {
                connectorsResourceCounter.set(desiredConnectors.size());
            }

            Set<String> deleteConnectorNames = new HashSet<>(runningConnectorNames);
            deleteConnectorNames.removeAll(desiredConnectors.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));
            log.debug("{}: {} cluster: delete connectors: {}", reconciliation, kind(), deleteConnectorNames);
            Stream<Future<Void>> deletionFutures = deleteConnectorNames.stream().map(connectorName ->
                    reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connectorName, null)
            );

            log.debug("{}: {} cluster: required connectors: {}", reconciliation, kind(), desiredConnectors);
            Stream<Future<Void>> createUpdateFutures = desiredConnectors.stream()
                    .map(connector -> reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connector.getMetadata().getName(), connector));

            return CompositeFuture.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        }).recover(error -> {
            if (error instanceof ConnectTimeoutException) {
                Promise<Void> connectorStatuses = Promise.promise();
                log.warn("{}: Failed to connect to the REST API => trying to update the connector status", reconciliation);

                connectorOperator.listAsync(namespace, Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build()))
                        .compose(connectors -> CompositeFuture.join(
                                connectors.stream().map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, error))
                                        .collect(Collectors.toList())
                        ))
                        .onComplete(ignore -> connectorStatuses.fail(error));

                return connectorStatuses.future();
            } else {
                return Future.failedFuture(error);
            }
        });
    }

    protected KafkaConnectApi getKafkaConnectApi() {
        return connectClientProvider.apply(vertx);
    }

    /*test*/ Future<Void> reconcileConnectorAndHandleResult(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        Promise<Void> reconciliationResult = Promise.promise();

        connectorsReconciliationsCounter.increment();
        Timer.Sample connectorsReconciliationsTimerSample = Timer.start(metrics.meterRegistry());

        reconcileConnector(reconciliation, host, apiClient, useResources, connectorName, connector)
                .onComplete(result -> {
                    connectorsReconciliationsTimerSample.stop(connectorsReconciliationsTimer);

                    if (result.succeeded())    {
                        connectorsSuccessfulReconciliationsCounter.increment();
                        reconciliationResult.complete();
                    } else {
                        connectorsFailedReconciliationsCounter.increment();
                        reconciliationResult.fail(result.cause());
                    }
                });

        return reconciliationResult.future();
    }

    private Future<Void> reconcileConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        if (connector == null) {
            if (useResources) {
                log.info("{}: deleting connector: {}", reconciliation, connectorName);
                return apiClient.delete(host, port, connectorName);
            } else {
                return Future.succeededFuture();
            }
        } else {
            log.info("{}: creating/updating connector: {}", reconciliation, connectorName);
            if (connector.getSpec() == null) {
                return maybeUpdateConnectorStatus(reconciliation, connector, null,
                        new InvalidResourceException("spec property is required"));
            }
            if (!useResources) {
                return maybeUpdateConnectorStatus(reconciliation, connector, null,
                        new NoSuchResourceException(reconciliation.kind() + " " + reconciliation.name() + " is not configured with annotation " + Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES));
            } else {
                Promise<Void> promise = Promise.promise();
                maybeCreateOrUpdateConnector(reconciliation, host, apiClient, connectorName, connector.getSpec())
                        .onComplete(result -> {
                            if (result.succeeded()) {
                                maybeUpdateConnectorStatus(reconciliation, connector, result.result(), null)
                                    .onComplete(promise);
                            } else {
                                maybeUpdateConnectorStatus(reconciliation, connector, result.result(), result.cause())
                                    .onComplete(promise);
                            }
                        });
                return promise.future();
            }
        }
    }

    /**
     * Try to get the current connector config. If the connector does not exist, or its config differs from the 
     * {@code connectorSpec}'s, then call
     * {@link #createOrUpdateConnector(Reconciliation, String, KafkaConnectApi, String, KafkaConnectorSpec)}
     * otherwise, just return the connectors current state.
     * @param reconciliation The reconciliation.
     * @param host The REST API host.
     * @param apiClient The client instance.
     * @param connectorName The connector name.
     * @param connectorSpec The desired connector spec.
     * @return A Future whose result, when successfully completed, is a map of the current connector state.
     */
    protected Future<Map<String, Object>> maybeCreateOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                                                       String connectorName, KafkaConnectorSpec connectorSpec) {
        return apiClient.getConnectorConfig(new BackOff(200L, 2, 6), host, port, connectorName).compose(
            config -> {
                if (!needsReconfiguring(reconciliation, connectorName, connectorSpec, config)) {
                    log.debug("{}: Connector {} exists and has desired config, {}=={}", reconciliation, connectorName, connectorSpec.getConfig(), config);
                    return apiClient.status(host, port, connectorName)
                        .compose(status -> {
                            return pauseResume(reconciliation, host, apiClient, connectorName, connectorSpec, status);
                        });
                } else {
                    log.debug("{}: Connector {} exists but does not have desired config, {}!={}", reconciliation, connectorName, connectorSpec.getConfig(), config);
                    return createOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec);
                }
            },
            error -> {
                if (error instanceof ConnectRestException
                        && ((ConnectRestException) error).getStatusCode() == 404) {
                    log.debug("{}: Connector {} does not exist", reconciliation, connectorName);
                    return createOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec);
                } else {
                    return Future.failedFuture(error);
                }
            });
    }

    private boolean needsReconfiguring(Reconciliation reconciliation, String connectorName,
                                       KafkaConnectorSpec connectorSpec,
                                       Map<String, String> actual) {
        Map<String, String> desired = new HashMap<>(connectorSpec.getConfig().size());
        // The actual which comes from Connect API includes tasks.max, connector.class and name,
        // which connectorSpec.getConfig() does not
        if (connectorSpec.getTasksMax() != null) {
            desired.put("tasks.max", connectorSpec.getTasksMax().toString());
        }
        desired.put("name", connectorName);
        desired.put("connector.class", connectorSpec.getClassName());
        for (Map.Entry<String, Object> entry : connectorSpec.getConfig().entrySet()) {
            desired.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
        }
        if (log.isDebugEnabled()) {
            log.debug("{}: Desired: {}", reconciliation, new TreeMap<>(desired));
            log.debug("{}: Actual:  {}", reconciliation, new TreeMap<>(actual));
        }
        return !desired.equals(actual);
    }

    protected Future<Map<String, Object>> createOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                                                  String connectorName, KafkaConnectorSpec connectorSpec) {
        return apiClient.createOrUpdatePutRequest(host, port, connectorName, asJson(connectorSpec))
            .compose(ignored -> apiClient.statusWithBackOff(new BackOff(200L, 2, 10), host, port,
                    connectorName))
            .compose(status -> {
                return pauseResume(reconciliation, host, apiClient, connectorName, connectorSpec, status);
            });
    }

    private Future<Map<String, Object>> pauseResume(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, KafkaConnectorSpec connectorSpec, Map<String, Object> status) {
        Object path = ((Map) status.getOrDefault("connector", emptyMap())).get("state");
        if (!(path instanceof String)) {
            return Future.failedFuture("JSON response lacked $.connector.state");
        } else {
            String state = (String) path;
            boolean shouldPause = Boolean.TRUE.equals(connectorSpec.getPause());
            if ("RUNNING".equals(state) && shouldPause) {
                log.debug("{}: Pausing connector {}", reconciliation, connectorName);
                return apiClient.pause(host, port,
                        connectorName)
                        .compose(ignored ->
                                apiClient.status(host, port,
                                        connectorName));
            } else if ("PAUSED".equals(state) && !shouldPause) {
                log.debug("{}: Resuming connector {}", reconciliation, connectorName);
                return apiClient.resume(host, port,
                        connectorName)
                        .compose(ignored ->
                                apiClient.status(host, port,
                                        connectorName));

            } else {
                return Future.succeededFuture(status);
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

    Future<Void> maybeUpdateConnectorStatus(Reconciliation reconciliation, KafkaConnector connector, Map<String, Object> statusResult, Throwable error) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        if (error != null) {
            log.warn("{}: Error reconciling connector {}", reconciliation, connector.getMetadata().getName(), error);
        }
        StatusUtils.setStatusConditionAndObservedGeneration(connector, status, error != null ? Future.failedFuture(error) : Future.succeededFuture());
        status.setConnectorStatus(statusResult);

        status.setTasksMax(getActualTaskCount(connector, statusResult));

        return maybeUpdateStatusCommon(connectorOperator, connector, reconciliation, status,
            (connector1, status1) -> {
                return new KafkaConnectorBuilder(connector1).withStatus(status1).build();
            });
    }

    /**
     * The tasksMax are mirrored in the KafkaConnector.status where they are used by the scale subresource.
     * However, .spec.tasksMax is not always set and has no default value in Strimzi (only in Kafka Connect). So when
     * it is not set, we try to count the tasks from the status. And if these are missing as well, we just set it to 0.
     *
     * @param connector         The KafkaConnector instance of the reconciled connector
     * @param statusResult      The status from the Connect REST API
     * @return                  Number of tasks which should be set in the status
     */
    protected int getActualTaskCount(KafkaConnector connector, Map<String, Object> statusResult)  {
        if (connector.getSpec() != null
                && connector.getSpec().getTasksMax() != null)  {
            return connector.getSpec().getTasksMax();
        } else if (statusResult != null
                && statusResult.containsKey("tasks")
                && statusResult.get("tasks") instanceof List) {
            return ((List) statusResult.get("tasks")).size();
        } else {
            return 0;
        }
    }

    protected JsonObject asJson(KafkaConnectorSpec spec) {
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

        if (spec.getTasksMax() != null) {
            connectorConfigJson.put("tasks.max", spec.getTasksMax());
        }

        return connectorConfigJson.put("connector.class", spec.getClassName());
    }

    /**
     * Updates the Status field of the KafkaConnect or KafkaConnector CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param resource The CR of KafkaConnect or KafkaConnector
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaConnectStatus or KafkaConnectorStatus which should be set
     *
     * @return
     */
    protected <T extends CustomResource & HasStatus<S>, S extends Status, L extends CustomResourceList<T>, D extends Doneable<T>> Future<Void>
        maybeUpdateStatusCommon(CrdOperator<KubernetesClient, T, L, D> resourceOperator,
                                T resource,
                                Reconciliation reconciliation,
                                S desiredStatus,
                                BiFunction<T, S, T> copyWithStatus) {
        Promise<Void> updateStatusPromise = Promise.promise();

        resourceOperator.getAsync(resource.getMetadata().getNamespace(), resource.getMetadata().getName()).onComplete(getRes -> {
            if (getRes.succeeded()) {
                T fetchedResource = getRes.result();

                if (fetchedResource != null) {
                    if ((!(fetchedResource instanceof KafkaConnector))
                            && (!(fetchedResource instanceof KafkaMirrorMaker2))
                            && StatusUtils.isResourceV1alpha1(fetchedResource)) {
                        log.warn("{}: {} {} needs to be upgraded from version {} to 'v1beta1' to use the status field",
                                reconciliation, fetchedResource.getKind(), fetchedResource.getMetadata().getName(), fetchedResource.getApiVersion());
                        updateStatusPromise.complete();
                    } else {
                        S currentStatus = fetchedResource.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            T resourceWithNewStatus = copyWithStatus.apply(fetchedResource, desiredStatus);

                            resourceOperator.updateStatusAsync(resourceWithNewStatus).onComplete(updateRes -> {
                                if (updateRes.succeeded()) {
                                    log.debug("{}: Completed status update", reconciliation);
                                    updateStatusPromise.complete();
                                } else {
                                    log.error("{}: Failed to update status", reconciliation, updateRes.cause());
                                    updateStatusPromise.fail(updateRes.cause());
                                }
                            });
                        } else {
                            log.debug("{}: Status did not change", reconciliation);
                            updateStatusPromise.complete();
                        }
                    }
                } else {
                    log.error("{}: Current {} resource not found", reconciliation, resource.getKind());
                    updateStatusPromise.fail("Current " + resource.getKind() + " resource not found");
                }
            } else {
                log.error("{}: Failed to get the current {} resource and its status", reconciliation, resource.getKind(), getRes.cause());
                updateStatusPromise.fail(getRes.cause());
            }
        });

        return updateStatusPromise.future();
    }
}
