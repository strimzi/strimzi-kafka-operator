/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.netty.channel.ConnectTimeoutException;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, KafkaConnectSpec, KafkaConnectStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectAssemblyOperator.class.getName());

    private final ConnectBuildOperator connectBuildOperator;

    /**
     * Constructor
     *
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    /**
     * Constructor which allows providing custom implementation of the Kafka Connect Client. This is used in tests.
     *
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy
     *                                  and the secrets.
     * @param connectClientProvider     Provider of the Kafka Connect client
     */
    protected KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        this(vertx, pfa, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
    }

    /**
     * Constructor which allows providing custom implementation of the Kafka Connect Client and port on which the Kafka
     * Connect REST API is listening. This is used in tests.
     *
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy
     *                                  and the secrets.
     * @param connectClientProvider     Provider of the Kafka Connect client
     * @param port                      Port of the Kafka Connect REST API
     */
    protected KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider, int port) {
        super(vertx, pfa, KafkaConnect.RESOURCE_KIND, supplier.connectOperator, supplier, config, connectClientProvider, port);

        this.connectBuildOperator = new ConnectBuildOperator(pfa, supplier, config);
    }

    @Override
    protected Future<KafkaConnectStatus> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        KafkaConnectCluster connect;
        KafkaConnectBuild build;
        KafkaConnectStatus kafkaConnectStatus = new KafkaConnectStatus();
        try {
            connect = KafkaConnectCluster.fromCrd(reconciliation, kafkaConnect, versions, sharedEnvironmentProvider);
            build = KafkaConnectBuild.fromCrd(reconciliation, kafkaConnect, versions, sharedEnvironmentProvider);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, e);
            return Future.failedFuture(new ReconciliationException(kafkaConnectStatus, e));
        }

        Promise<KafkaConnectStatus> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();

        Map<String, String> podAnnotations = new HashMap<>();
        Map<String, String> controllerAnnotations = new HashMap<>();

        final boolean hasZeroReplicas = connect.getReplicas() == 0;
        final AtomicReference<String> image = new AtomicReference<>();
        final AtomicReference<String> desiredLogging = new AtomicReference<>();
        final AtomicReference<Deployment> deployment = new AtomicReference<>();
        final AtomicReference<StrimziPodSet> podSet = new AtomicReference<>();
        String initCrbName = KafkaConnectResources.initContainerClusterRoleBindingName(kafkaConnect.getMetadata().getName(), namespace);
        ClusterRoleBinding initCrb = connect.generateClusterRoleBinding();

        LOGGER.debugCr(reconciliation, "Creating or updating Kafka Connect cluster");

        controllerResources(reconciliation, connect, deployment, podSet)
                .compose(i -> connectServiceAccount(reconciliation, namespace, KafkaConnectResources.serviceAccountName(connect.getCluster()), connect))
                .compose(i -> connectInitClusterRoleBinding(reconciliation, initCrbName, initCrb))
                .compose(i -> connectNetworkPolicy(reconciliation, namespace, connect, isUseResources(kafkaConnect)))
                .compose(i -> manualRollingUpdate(reconciliation, connect))
                .compose(i -> connectBuildOperator.reconcile(reconciliation, namespace, activeController(deployment.get(), podSet.get()), build))
                .compose(buildInfo -> {
                    if (buildInfo != null) {
                        podAnnotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildInfo.buildRevision());
                        controllerAnnotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildInfo.buildRevision());
                        controllerAnnotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, buildInfo.image());
                        image.set(buildInfo.image());
                    }
                    return Future.succeededFuture();
                })
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generateHeadlessService()))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, connect))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(connect.logging().configMapKey());
                    podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    desiredLogging.set(logging);
                    return configMapOperations.reconcile(reconciliation, namespace, logAndMetricsConfigMap.getMetadata().getName(), logAndMetricsConfigMap);
                })
                .compose(i -> ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperations, connect))
                .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generatePodDisruptionBudget()))
                .compose(i -> generateAuthHash(namespace, kafkaConnect.getSpec()))
                .compose(hash -> {
                    podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    return Future.succeededFuture();
                })
                .compose(i -> new KafkaConnectMigration(
                                reconciliation,
                                connect,
                                controllerAnnotations,
                                podAnnotations,
                                operationTimeoutMs,
                                pfa.isOpenshift(),
                                imagePullPolicy,
                                imagePullSecrets,
                                image.get(),
                                deploymentOperations,
                                podSetOperations,
                                podOperations
                        )
                        .migrateFromDeploymentToStrimziPodSets(deployment.get(), podSet.get()))
                .compose(i -> reconcilePodSet(reconciliation, connect, podAnnotations, controllerAnnotations, image.get()))
                .compose(i -> reconcileConnectors(reconciliation, kafkaConnect, kafkaConnectStatus, hasZeroReplicas, desiredLogging.get(), connect.defaultLogConfig()))
                .onComplete(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult.cause());

                    if (!hasZeroReplicas) {
                        kafkaConnectStatus.setUrl(KafkaConnectResources.url(connect.getCluster(), namespace, KafkaConnectCluster.REST_API_PORT));
                    }

                    kafkaConnectStatus.setReplicas(connect.getReplicas());
                    kafkaConnectStatus.setLabelSelector(connect.getSelectorLabels().toSelectorString());

                    if (reconciliationResult.succeeded())   {
                        createOrUpdatePromise.complete(kafkaConnectStatus);
                    } else {
                        createOrUpdatePromise.fail(new ReconciliationException(kafkaConnectStatus, reconciliationResult.cause()));
                    }
                });

        return createOrUpdatePromise.future();
    }

    private Future<Void> controllerResources(Reconciliation reconciliation,
                                             KafkaConnectCluster connect,
                                             AtomicReference<Deployment> deploymentReference,
                                             AtomicReference<StrimziPodSet> podSetReference)   {
        return Future
                .join(deploymentOperations.getAsync(reconciliation.namespace(), connect.getComponentName()), podSetOperations.getAsync(reconciliation.namespace(), connect.getComponentName()))
                .compose(res -> {
                    deploymentReference.set(res.resultAt(0));
                    podSetReference.set(res.resultAt(1));

                    return Future.succeededFuture();
                });
    }

    /**
     * Finds and returns the correct controller resource:
     *     - If stable identities are enabled and PodSet exists, returns PodSet
     *     - If stable identities are enabled and PodSet is null, we might be in the middle of migration, so it returns the Deployment (which in worst case is null which is fine)
     *     - If stable identities are disabled and Deployment exists, returns Deployment
     *     - If stable identities are disabled and Deployment is null, we might be in the middle of migration, so it returns the PodSet (which in worst case is null which is fine)
     *
     * @param deployment    Deployment resource
     * @param podSet        PodSet resource
     *
     * @return  The active controller resource
     */
    private HasMetadata activeController(Deployment deployment, StrimziPodSet podSet)  {
        // TODO: Do we really need this?
        return podSet != null ? podSet : deployment;
    }

    private Future<Void> reconcilePodSet(Reconciliation reconciliation,
                                         KafkaConnectCluster connect,
                                         Map<String, String> podAnnotations,
                                         Map<String, String> podSetAnnotations,
                                         String customContainerImage)  {
        return podSetOperations.reconcile(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.generatePodSet(connect.getReplicas(), podSetAnnotations, podAnnotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, customContainerImage))
                .compose(reconciliationResult -> {
                    KafkaConnectRoller roller = new KafkaConnectRoller(reconciliation, connect, operationTimeoutMs, podOperations);
                    return roller.maybeRoll(PodSetUtils.podNames(reconciliationResult.resource()), pod -> KafkaConnectRoller.needsRollingRestart(reconciliationResult.resource(), pod));
                })
                .compose(i -> podSetOperations.readiness(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs));
    }

    @Override
    protected KafkaConnectStatus createStatus(KafkaConnect ignored) {
        return new KafkaConnectStatus();
    }

    @Override
    public void reconcileThese(String trigger, Set<NamespaceAndName> desiredNames, String namespace, Handler<AsyncResult<Void>> handler) {
        super.reconcileThese(trigger, desiredNames, namespace, ignore -> {
            List<String> connects = desiredNames.stream().map(NamespaceAndName::getName).collect(Collectors.toList());
            LabelSelectorRequirement requirement = new LabelSelectorRequirement(Labels.STRIMZI_CLUSTER_LABEL, "In", connects);
            LabelSelector connectorsSelector = new LabelSelector(List.of(requirement), null);
            connectorOperator.listAsync(namespace, connectorsSelector)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            metrics().resetConnectorsCounters(namespace);
                            ar.result().forEach(connector -> {
                                metrics().connectorsResourceCounter(connector.getMetadata().getNamespace()).incrementAndGet();
                                if (isPaused(connector.getStatus())) {
                                    metrics().pausedConnectorsResourceCounter(connector.getMetadata().getNamespace()).incrementAndGet();
                                }
                            });
                            handler.handle(Future.succeededFuture());
                        } else {
                            handler.handle(ar.map((Void) null));
                        }
                    });
        });
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference and
     * updates the status of all KafkaConnector resources to mark that they are now orphaned (have no matching Connect
     * cluster).
     *
     * @param reconciliation    The Reconciliation identification
     *
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return updateConnectorsThatConnectClusterWasDeleted(reconciliation)
                .compose(i -> ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaConnectResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null))
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }

    /**
     * When the KafkaConnect cluster is deleted, thi method updates all KafkaConnector resources belonging to it with
     * the error that they do not have matching KafkaConnect cluster.
     *
     * @param reconciliation    Reconciliation marker
     *
     * @return  Future indicating that all connectors have been updated
     */
    private Future<Void> updateConnectorsThatConnectClusterWasDeleted(Reconciliation reconciliation) {
        // When deleting KafkaConnect we need to update the status of all its KafkaConnector
        return connectorOperator.listAsync(reconciliation.namespace(), Labels.forStrimziCluster(reconciliation.name())).compose(connectors -> {
            List<Future<Void>> connectorFutures = new ArrayList<>();
            for (KafkaConnector connector : connectors) {
                connectorFutures.add(maybeUpdateConnectorStatus(reconciliation, connector, null,
                    noConnectCluster(reconciliation.namespace(), reconciliation.name())));
            }
            return Future.join(connectorFutures);
        }).map((Void) null);
    }

    /**
     * Generates a hash from the trusted TLS certificates that can be used to spot if it has changed.
     *
     * @param namespace          Namespace of the Connect cluster
     * @param kafkaConnectSpec   KafkaConnectSpec object
     * @return                   Future for tracking the asynchronous result of generating the TLS auth hash
     */
    Future<Integer> generateAuthHash(String namespace, KafkaConnectSpec kafkaConnectSpec) {
        KafkaClientAuthentication auth = kafkaConnectSpec.getAuthentication();
        List<CertSecretSource> trustedCertificates = kafkaConnectSpec.getTls() == null ? Collections.emptyList() : kafkaConnectSpec.getTls().getTrustedCertificates();
        return VertxUtil.authTlsHash(secretOperations, namespace, auth, trustedCertificates);
    }

    /**
     * Reconcile all the connectors selected by the given connect instance, updated each connectors status with the result.
     * @param reconciliation The reconciliation
     * @param connect The connector
     * @param connectStatus Status of the KafkaConnect  resource (will be used to set the available
     *                      connector plugins)
     * @param scaledToZero  Indicated whether the related Connect cluster is currently scaled to 0 replicas
     * @return A future, failed if any of the connectors' statuses could not be updated.
     */
    private Future<Void> reconcileConnectors(Reconciliation reconciliation, KafkaConnect connect, KafkaConnectStatus connectStatus, boolean scaledToZero, String desiredLogging, OrderedProperties defaultLogging) {
        String connectName = connect.getMetadata().getName();
        String namespace = connect.getMetadata().getNamespace();
        String host = KafkaConnectResources.qualifiedServiceName(connectName, namespace);

        if (!isUseResources(connect))    {
            return Future.succeededFuture();
        }

        if (scaledToZero)   {
            return connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())
                .compose(connectors -> Future.join(
                    connectors.stream()
                        .filter(connector -> !Annotations.isReconciliationPausedWithAnnotation(connector))
                        .map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, zeroReplicas(namespace, connectName)))
                        .collect(Collectors.toList())
                ))
                .map((Void) null);
        }

        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);

        return Future.join(
                apiClient.list(reconciliation, host, port),
                connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build()),
                apiClient.listConnectorPlugins(reconciliation, host, port),
                apiClient.updateConnectLoggers(reconciliation, host, port, desiredLogging, defaultLogging)
        ).compose(cf -> {
            List<String> runningConnectorNames = cf.resultAt(0);
            List<KafkaConnector> desiredConnectors = cf.resultAt(1);
            List<ConnectorPlugin> connectorPlugins = cf.resultAt(2);

            LOGGER.debugCr(reconciliation, "Setting list of connector plugins in Kafka Connect status");
            connectStatus.setConnectorPlugins(connectorPlugins);

            Set<String> deleteConnectorNames = new HashSet<>(runningConnectorNames);
            deleteConnectorNames.removeAll(desiredConnectors.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));
            LOGGER.debugCr(reconciliation, "{} cluster: delete connectors: {}", kind(), deleteConnectorNames);
            Stream<Future<Void>> deletionFutures = deleteConnectorNames.stream().map(connectorName ->
                    reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connectorName, null)
            );

            LOGGER.debugCr(reconciliation, "{} cluster: required connectors: {}", kind(), desiredConnectors);
            Stream<Future<Void>> createUpdateFutures = desiredConnectors.stream()
                    .map(connector -> reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connector.getMetadata().getName(), connector));

            return Future.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        }).recover(error -> {
            if (error instanceof ConnectTimeoutException) {
                Promise<Void> connectorStatuses = Promise.promise();
                LOGGER.warnCr(reconciliation, "Failed to connect to the REST API => trying to update the connector status");

                connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())
                        .compose(connectors -> Future.join(
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

    private boolean isPaused(KafkaConnectorStatus status) {
        return status != null && status.getConditions().stream().anyMatch(condition -> "ReconciliationPaused".equals(condition.getType()));
    }

    /**
     * Create Kubernetes watch for KafkaConnector resources.
     *
     * @param namespace     Namespace where to watch for the resources
     *
     * @return  A future which completes when the watcher has been created
     */
    public Future<ReconnectingWatcher<KafkaConnector>> createConnectorWatch(String namespace) {
        return VertxUtil.async(vertx, () -> new ReconnectingWatcher<>(connectorOperator, KafkaConnector.RESOURCE_KIND, namespace, null, this::connectorEventHandler));
    }

    /**
     * Event handler called when the KafkaConnector watch receives an event.
     *
     * @param action    An Action describing the type of the event
     * @param resource  The resource for which the event was triggered
     */
    private void connectorEventHandler(Watcher.Action action, KafkaConnector resource) {
        String connectorName = resource.getMetadata().getName();
        String namespace = resource.getMetadata().getNamespace();
        String connectorKind = resource.getKind();
        String connectName = resource.getMetadata().getLabels() == null ? null : resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);

        switch (action) {
            case ADDED, DELETED, MODIFIED -> {
                if (connectName != null) {
                    // Check whether a KafkaConnect exists
                    resourceOperator.getAsync(namespace, connectName)
                            .compose(connect -> {
                                KafkaConnectApi apiClient = connectClientProvider.apply(vertx);
                                if (connect == null) {
                                    Reconciliation r = new Reconciliation("connector-watch", kind(),
                                            resource.getMetadata().getNamespace(), connectName);
                                    updateStatus(r, noConnectCluster(namespace, connectName), resource, connectorOperator);
                                    LOGGER.infoCr(r, "{} {} in namespace {} was {}, but Connect cluster {} does not exist", connectorKind, connectorName, namespace, action, connectName);
                                    return Future.succeededFuture();
                                } else {
                                    // grab the lock and call reconcileConnectors()
                                    // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                    Reconciliation reconciliation = new Reconciliation("connector-watch", kind(), resource.getMetadata().getNamespace(), connectName);

                                    if (!Util.matchesSelector(selector(), connect)) {
                                        LOGGER.debugCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} does not match label selector {} and will be ignored", connectorKind, connectorName, namespace, action, connectName, selector());
                                        return Future.succeededFuture();
                                    } else if (connect.getSpec() != null && connect.getSpec().getReplicas() == 0) {
                                        LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} has 0 replicas", connectorKind, connectorName, namespace, action, connectName);
                                        updateStatus(reconciliation, zeroReplicas(namespace, connectName), resource, connectorOperator);
                                        return Future.succeededFuture();
                                    } else {
                                        LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", connectorKind, connectorName, namespace, action);

                                        return withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                () -> reconcileConnectorAndHandleResult(reconciliation,
                                                                KafkaConnectResources.qualifiedServiceName(connectName, namespace), apiClient,
                                                                isUseResources(connect),
                                                                resource.getMetadata().getName(), action == Watcher.Action.DELETED ? null : resource)
                                                        .compose(reconcileResult -> {
                                                            LOGGER.infoCr(reconciliation, "reconciled");
                                                            return Future.succeededFuture(reconcileResult);
                                                        }));
                                    }
                                }
                            });
                } else {
                    updateStatus(new Reconciliation("connector-watch", kind(), resource.getMetadata().getNamespace(), null),
                            new InvalidResourceException("Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No connect cluster in which to create this connector."), resource, connectorOperator);
                }
            }
            case ERROR ->
                    LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, namespace), "Failed {} {} in namespace {} ", connectorKind, connectorName, namespace);
            default ->
                    LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, namespace), "Unknown action: {} {} in namespace {}", connectorKind, connectorName, namespace);
        }
    }
}
