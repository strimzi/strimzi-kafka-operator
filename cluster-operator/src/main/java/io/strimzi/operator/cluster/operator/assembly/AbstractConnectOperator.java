/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ConnectTimeoutException;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.connect.AbstractKafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatusBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectorConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_TASK;
import static java.util.Collections.emptyMap;

/**
 * Abstract operator for managing Connect based resources (Connect and Mirror Maker 2)
 *
 * @param <C>   Kubernetes client type
 * @param <T>   Custom Resource type
 * @param <L>   Custom Resource List type
 * @param <R>   Custom Resource "Resource" type
 * @param <P>   Custom Resource Spec type
 * @param <S>   Custom Resource Status type
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:CyclomaticComplexity"})
public abstract class AbstractConnectOperator<C extends KubernetesClient, T extends CustomResource<P, S>,
        L extends DefaultKubernetesResourceList<T>, R extends Resource<T>, P extends AbstractKafkaConnectSpec, S extends KafkaConnectStatus>
        extends AbstractOperator<T, P, S, CrdOperator<C, T, L>> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractConnectOperator.class.getName());

    private final boolean isNetworkPolicyGeneration;
    protected final Function<Vertx, KafkaConnectApi> connectClientProvider;
    protected final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator;
    protected final ImagePullPolicy imagePullPolicy;
    protected final DeploymentOperator deploymentOperations;
    protected final StrimziPodSetOperator podSetOperations;
    protected final PodOperator podOperations;
    protected final ConfigMapOperator configMapOperations;
    protected final ClusterRoleBindingOperator clusterRoleBindingOperations;
    protected final ServiceOperator serviceOperations;
    protected final SecretOperator secretOperations;
    protected final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    protected final NetworkPolicyOperator networkPolicyOperator;
    protected final List<LocalObjectReference> imagePullSecrets;
    protected final long operationTimeoutMs;
    protected final String operatorNamespace;
    protected final Labels operatorNamespaceLabels;
    protected final PlatformFeaturesAvailability pfa;
    protected final ServiceAccountOperator serviceAccountOperations;
    protected final KafkaVersion.Lookup versions;
    protected final SharedEnvironmentProvider sharedEnvironmentProvider;
    private final int port;

    /**
     * Constructor
     *
     * @param vertx                     Vert.x instance
     * @param pfa                       PlatformFeaturesAvailability describing the platform features
     * @param kind                      The kind of the custom resource which will be managed
     * @param resourceOperator          The resource operator for the custom resource
     * @param supplier                  The supplier of resource operators
     * @param config                    Cluster operator configuration
     * @param connectClientProvider     Provider of the Kafka Connect REST API client
     * @param port                      Port number on which the Connect REST API is listening
     */
    public AbstractConnectOperator(Vertx vertx, PlatformFeaturesAvailability pfa, String kind,
                                   CrdOperator<C, T, L> resourceOperator,
                                   ResourceOperatorSupplier supplier, ClusterOperatorConfig config,
                                   Function<Vertx, KafkaConnectApi> connectClientProvider,
                                   int port) {
        super(vertx, kind, resourceOperator, new ConnectOperatorMetricsHolder(kind, config.getCustomResourceSelector(), supplier.metricsProvider), config.getCustomResourceSelector());

        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.deploymentOperations = supplier.deploymentOperations;
        this.podSetOperations = supplier.strimziPodSetOperator;
        this.podOperations = supplier.podOperations;
        this.connectorOperator = supplier.kafkaConnectorOperator;
        this.connectClientProvider = connectClientProvider;
        this.configMapOperations = supplier.configMapOperations;
        this.clusterRoleBindingOperations = supplier.clusterRoleBindingOperator;
        this.serviceOperations = supplier.serviceOperations;
        this.secretOperations = supplier.secretOperations;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.pfa = pfa;
        this.versions = config.versions();
        this.sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;
        this.port = port;
    }

    @Override
    public ConnectOperatorMetricsHolder metrics()   {
        // We have to check the type because of Spotbugs
        if (metrics instanceof ConnectOperatorMetricsHolder) {
            return (ConnectOperatorMetricsHolder) metrics;
        } else {
            throw new RuntimeException("MetricsHolder in AbstractConnectOperator should be always of type ConnectOperatorMetricsHolder");
        }
    }

    /**
     * Checks whether the use of KafkaConnector resources is enabled for this cluster or not
     *
     * @param connect   The Connect custom resource
     *
     * @return  True if the connector operator is enabled and KafkaConnector resources should be used. False otherwise.
     */
    public static boolean isUseResources(HasMetadata connect) {
        return Annotations.booleanAnnotation(connect, Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, false);
    }

    /**
     * Creates an exception indicating that the KafkaConnect resource which would match given KafkaConnector resource
     * does not exist.
     *
     * @param connectNamespace  Namespace where the KafkaConnect resource is missing
     * @param connectName       Name of the missing KafkaConnect resource
     *
     * @return  NoSuchResourceException indicating that the Connect cluster is missing
     */
    static NoSuchResourceException noConnectCluster(String connectNamespace, String connectName) {
        return new NoSuchResourceException(
                "KafkaConnect resource '" + connectName + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + connectNamespace + ".");
    }

    /**
     * Creates an exception indicating that the connector cannot run because its Connect cluster has 0 replicas.
     *
     * @param connectNamespace  Namespace where the KafkaConnect resource exists
     * @param connectName       Name of the KafkaConnect resource
     *
     * @return  NoSuchResourceException indicating that the Connect cluster is missing
     */
    static RuntimeException zeroReplicas(String connectNamespace, String connectName) {
        return new RuntimeException(
                "Kafka Connect cluster '" + connectName + "' in namespace " + connectNamespace + " has 0 replicas.");
    }

    /**
     * Reconciles the ServiceAccount for the Connect cluster.
     *
     * @param reconciliation       The reconciliation
     * @param namespace            Namespace of the Connect cluster
     * @param name                 ServiceAccount name
     * @param connect              KafkaConnectCluster object
     * @return                     Future for tracking the asynchronous result of reconciling the ServiceAccount
     */
    protected Future<ReconcileResult<ServiceAccount>> connectServiceAccount(Reconciliation reconciliation, String namespace, String name, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(reconciliation, namespace, name, connect.generateServiceAccount());
    }

    /**
     * Reconciles the NetworkPolicy for the Connect cluster.
     *
     * @param reconciliation       The reconciliation
     * @param namespace            Namespace of the Connect cluster
     * @param connect              KafkaConnectCluster object
     * @return                     Future for tracking the asynchronous result of reconciling the NetworkPolicy
     */
    protected Future<ReconcileResult<NetworkPolicy>> connectNetworkPolicy(Reconciliation reconciliation, String namespace, KafkaConnectCluster connect, boolean connectorOperatorEnabled) {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generateNetworkPolicy(connectorOperatorEnabled, operatorNamespace, operatorNamespaceLabels));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Executes manual rolling update of the Kafka Connect / MM2 Pods based on the annotation set by the user on the
     * StrimziPodSet or on the Pods.
     *
     * @param reconciliation    Reconciliation marker
     * @param connect           Instance of the Connect or MM2 cluster
     *
     * @return  Future which completes when the manual rolling update is done. Either when the pods marked with the
     *          annotation were rolled or when there is nothing to roll.
     */
    protected Future<Void> manualRollingUpdate(Reconciliation reconciliation, KafkaConnectCluster connect)  {
        return podSetOperations.getAsync(reconciliation.namespace(), connect.getComponentName())
                .compose(podSet -> {
                    if (podSet != null
                            && Annotations.booleanAnnotation(podSet, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                        // We should roll the whole PodSet -> we return all its pods without any need to list the Pods
                        return Future.succeededFuture(PodSetUtils.podNames(podSet));
                    } else {
                        // The PodSet is not annotated for rolling - but the Pods might be
                        return podOperations.listAsync(reconciliation.namespace(), connect.getSelectorLabels())
                                .compose(pods -> Future.succeededFuture(pods.stream().filter(pod -> Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)).map(pod -> pod.getMetadata().getName()).collect(Collectors.toList())));
                    }
                })
                .compose(podNamesToRoll -> {
                    if (!podNamesToRoll.isEmpty())  {
                        // There are some pods to roll
                        KafkaConnectRoller roller = new KafkaConnectRoller(reconciliation, connect, operationTimeoutMs, podOperations);
                        return roller.maybeRoll(podNamesToRoll, pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE));
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Generates a config map for metrics and logging information.
     *
     * @param reconciliation       The reconciliation
     * @param namespace            Namespace of the Connect cluster
     * @param kafkaConnectCluster  KafkaConnectCluster object
     * @return                     Future for tracking the asynchronous result of getting the metrics and logging config map
     */
    protected Future<ConfigMap> generateMetricsAndLoggingConfigMap(Reconciliation reconciliation, String namespace, KafkaConnectCluster kafkaConnectCluster) {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperations, kafkaConnectCluster.logging(), kafkaConnectCluster.metrics())
                .compose(metricsAndLoggingCm -> Future.succeededFuture(kafkaConnectCluster.generateMetricsAndLogConfigMap(metricsAndLoggingCm)));
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
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, T connect, S connectStatus, boolean scaledToZero, String desiredLogging, OrderedProperties defaultLogging) {
        String connectName = connect.getMetadata().getName();
        String namespace = connect.getMetadata().getNamespace();
        String host = KafkaConnectResources.qualifiedServiceName(connectName, namespace);

        if (!isUseResources(connect))    {
            return Future.succeededFuture();
        }

        if (scaledToZero)   {
            return connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())
                    .compose(connectors -> Future.join(
                            connectors.stream().map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, zeroReplicas(namespace, connectName)))
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

    protected KafkaConnectApi getKafkaConnectApi() {
        return connectClientProvider.apply(vertx);
    }

    /*test*/ Future<Void> reconcileConnectorAndHandleResult(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        Promise<Void> reconciliationResult = Promise.promise();

        metrics().connectorsReconciliationsCounter(reconciliation.namespace()).increment();
        Timer.Sample connectorsReconciliationsTimerSample = Timer.start(metrics().metricsProvider().meterRegistry());

        if (connector != null && Annotations.isReconciliationPausedWithAnnotation(connector)) {
            return maybeUpdateConnectorStatus(reconciliation, connector, null, null).compose(
                i -> {
                    connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));
                    metrics().connectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                    return Future.succeededFuture();
                }
            );
        }

        reconcileConnector(reconciliation, host, apiClient, useResources, connectorName, connector)
                .onComplete(result -> {
                    if (result.succeeded() && result.result() == null)  {
                        // The reconciliation succeeded, but there is no status to be set => we complete the reconciliation and return
                        // This normally means that the connector was deleted and there is no status to be set
                        metrics().connectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                        connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));
                        reconciliationResult.complete();
                    } else if (result.failed() && connector == null) {
                        // The reconciliation failed on connector deletion, so there is nowhere for status to be set => we complete the reconciliation and return
                        LOGGER.warnCr(reconciliation, "Error reconciling connector {}", connectorName, result.cause());
                        metrics().connectorsFailedReconciliationsCounter(reconciliation.namespace()).increment();
                        connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));

                        // We suppress the error to not fail Connect reconciliation just because of a failing connector
                        reconciliationResult.complete();
                    } else {
                        maybeUpdateConnectorStatus(reconciliation, connector, result.result(), result.cause())
                                .onComplete(statusResult -> {
                                    connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));

                                    if (result.succeeded() && statusResult.succeeded()) {
                                        metrics().connectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                                        reconciliationResult.complete();
                                    } else {
                                        // Reconciliation failed if either reconciliation or status update failed
                                        metrics().connectorsFailedReconciliationsCounter(reconciliation.namespace()).increment();

                                        // We suppress the error to not fail Connect reconciliation just because of a failing connector
                                        reconciliationResult.complete();
                                    }
                                });
                    }
                });

        return reconciliationResult.future();
    }

    private Future<ConnectorStatusAndConditions> reconcileConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        if (connector == null) {
            if (useResources) {
                LOGGER.infoCr(reconciliation, "deleting connector: {}", connectorName);
                return apiClient.delete(reconciliation, host, port, connectorName).mapEmpty();
            } else {
                return Future.succeededFuture();
            }
        } else {
            LOGGER.infoCr(reconciliation, "creating/updating connector: {}", connectorName);

            if (connector.getSpec() == null) {
                return Future.failedFuture(new InvalidResourceException("spec property is required"));
            }

            if (!useResources) {
                return Future.failedFuture(new NoSuchResourceException(reconciliation.kind() + " " + reconciliation.name() + " is not configured with annotation " + Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES));
            } else {
                return maybeCreateOrUpdateConnector(reconciliation, host, apiClient, connectorName, connector.getSpec(), connector);
            }
        }
    }

    /**
     * Try to get the current connector config. If the connector does not exist, or its config differs from the
     * {@code connectorSpec}'s, then call
     * {@link #createOrUpdateConnector(Reconciliation, String, KafkaConnectApi, String, KafkaConnectorSpec, KafkaConnectorConfiguration)}
     * otherwise, just return the connectors current state.
     * @param reconciliation The reconciliation.
     * @param host The REST API host.
     * @param apiClient The client instance.
     * @param connectorName The connector name.
     * @param connectorSpec The desired connector spec.
     * @param resource The resource that defines the connector.
     * @return A Future whose result, when successfully completed, is a ConnectorStatusAndConditions object containing the map of the current connector state plus any conditions that have arisen.
     */
    @SuppressWarnings({ "rawtypes" })
    protected Future<ConnectorStatusAndConditions> maybeCreateOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                                                                String connectorName, KafkaConnectorSpec connectorSpec, CustomResource resource) {
        KafkaConnectorConfiguration desiredConfig = new KafkaConnectorConfiguration(reconciliation, connectorSpec.getConfig().entrySet());

        return apiClient.getConnectorConfig(reconciliation, new BackOff(200L, 2, 6), host, port, connectorName).compose(
            currentConfig -> {
                if (!needsReconfiguring(reconciliation, connectorName, connectorSpec, desiredConfig.asOrderedProperties().asMap(), currentConfig)) {
                    LOGGER.debugCr(reconciliation, "Connector {} exists and has desired config, {}=={}", connectorName, desiredConfig.asOrderedProperties().asMap(), currentConfig);
                    return apiClient.status(reconciliation, host, port, connectorName)
                        .compose(status -> updateState(reconciliation, host, apiClient, connectorName, connectorSpec, status, new ArrayList<>()))
                        .compose(conditions -> maybeRestartConnector(reconciliation, host, apiClient, connectorName, resource, conditions))
                        .compose(conditions -> maybeRestartConnectorTask(reconciliation, host, apiClient, connectorName, resource, conditions))
                        .compose(conditions ->
                            apiClient.statusWithBackOff(reconciliation, new BackOff(200L, 2, 10), host, port, connectorName)
                                .compose(createConnectorStatusAndConditions(conditions)))
                        .compose(status -> autoRestartFailedConnectorAndTasks(reconciliation, host, apiClient, connectorName, connectorSpec, status, resource))
                        .compose(status -> updateConnectorTopics(reconciliation, host, apiClient, connectorName, status));
                } else {
                    LOGGER.debugCr(reconciliation, "Connector {} exists but does not have desired config, {}!={}", connectorName, desiredConfig.asOrderedProperties().asMap(), currentConfig);
                    return createOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec, desiredConfig)
                        .compose(createConnectorStatusAndConditions())
                        .compose(status -> updateConnectorTopics(reconciliation, host, apiClient, connectorName, status));
                }
            },
            error -> {
                if (error instanceof ConnectRestException
                        && ((ConnectRestException) error).getStatusCode() == 404) {
                    LOGGER.debugCr(reconciliation, "Connector {} does not exist", connectorName);
                    return createOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec, desiredConfig)
                        .compose(createConnectorStatusAndConditions())
                        .compose(status -> autoRestartFailedConnectorAndTasks(reconciliation, host, apiClient, connectorName, connectorSpec, status, resource))
                        .compose(status -> updateConnectorTopics(reconciliation, host, apiClient, connectorName, status));
                } else {
                    return Future.failedFuture(error);
                }
            });
    }

    private boolean needsReconfiguring(Reconciliation reconciliation, String connectorName,
                                       KafkaConnectorSpec connectorSpec,
                                       Map<String, String> desiredConfig,
                                       Map<String, String> actualConfig) {
        // The actual which comes from Connect API includes tasks.max, connector.class and name,
        // which connectorSpec.getConfig() does not
        if (connectorSpec.getTasksMax() != null) {
            desiredConfig.put("tasks.max", connectorSpec.getTasksMax().toString());
        }
        desiredConfig.put("name", connectorName);
        desiredConfig.put("connector.class", connectorSpec.getClassName());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debugCr(reconciliation, "Desired configuration for connector {}: {}", connectorName, new TreeMap<>(desiredConfig));
            LOGGER.debugCr(reconciliation, "Actual configuration for connector {}:  {}", connectorName, new TreeMap<>(actualConfig));
        }

        return !desiredConfig.equals(actualConfig);
    }

    protected Future<Map<String, Object>> createOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                                                  String connectorName, KafkaConnectorSpec connectorSpec, KafkaConnectorConfiguration desiredConfig) {
        return apiClient.createOrUpdatePutRequest(reconciliation, host, port, connectorName, asJson(connectorSpec, desiredConfig))
            .compose(ignored -> apiClient.statusWithBackOff(reconciliation, new BackOff(200L, 2, 10), host, port,
                    connectorName))
            .compose(status -> updateState(reconciliation, host, apiClient, connectorName, connectorSpec, status, new ArrayList<>()))
            .compose(ignored ->  apiClient.status(reconciliation, host, port, connectorName));
    }

    private Future<List<Condition>> updateState(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, KafkaConnectorSpec connectorSpec, Map<String, Object> status, List<Condition> conditions) {
        @SuppressWarnings({ "rawtypes" })
        Object path = ((Map) status.getOrDefault("connector", emptyMap())).get("state");
        if (!(path instanceof String state)) {
            return Future.failedFuture("JSON response lacked $.connector.state");
        } else {
            ConnectorState desiredState = connectorSpec.getState();
            @SuppressWarnings("deprecation")
            Boolean shouldPause = connectorSpec.getPause();
            ConnectorState targetState = desiredState != null ? desiredState :
                    Boolean.TRUE.equals(shouldPause) ? ConnectorState.PAUSED : ConnectorState.RUNNING;
            if (desiredState != null && shouldPause != null) {
                String message = "Both pause and state are set. Since pause is deprecated, state takes precedence " +
                        "so the connector will be " + targetState.toValue();
                LOGGER.warnCr(reconciliation, message);
                conditions.add(StatusUtils.buildWarningCondition("UpdateState", message));
            }
            Future<Void> future = Future.succeededFuture();
            switch (state) {
                case "RUNNING" -> {
                    if (targetState == ConnectorState.PAUSED) {
                        LOGGER.infoCr(reconciliation, "Pausing connector {}", connectorName);
                        future = apiClient.pause(reconciliation, host, port, connectorName);
                    } else if (targetState == ConnectorState.STOPPED) {
                        LOGGER.infoCr(reconciliation, "Stopping connector {}", connectorName);
                        future = apiClient.stop(reconciliation, host, port, connectorName);
                    }
                }
                case "PAUSED" -> {
                    if (targetState == ConnectorState.RUNNING) {
                        LOGGER.infoCr(reconciliation, "Resuming connector {}", connectorName);
                        future = apiClient.resume(reconciliation, host, port, connectorName);
                    } else if (targetState == ConnectorState.STOPPED) {
                        LOGGER.infoCr(reconciliation, "Stopping connector {}", connectorName);
                        future = apiClient.stop(reconciliation, host, port, connectorName);
                    }
                }
                case "STOPPED" -> {
                    if (targetState == ConnectorState.RUNNING) {
                        LOGGER.infoCr(reconciliation, "Resuming connector {}", connectorName);
                        future = apiClient.resume(reconciliation, host, port, connectorName);
                    } else if (targetState == ConnectorState.PAUSED) {
                        LOGGER.infoCr(reconciliation, "Pausing connector {}", connectorName);
                        future = apiClient.pause(reconciliation, host, port, connectorName);
                    }
                }
                default -> {
                    // Connectors can also be in the UNASSIGNED or RESTARTING state. We could transition directly
                    // from these states to PAUSED or STOPPED but as these are transient, and typically lead to
                    // RUNNING, we ignore them here.
                    return Future.succeededFuture(conditions);
                }
            }
            return future.compose(ignored -> Future.succeededFuture(conditions));
        }
    }

    /**
     * Handles auto-restarting of the connectors and managing the auto-restart status. It checks that current state of
     * the connector and its tasks. If it is failing, it will restart them in periodic intervals with backoff. If the connector is stable after the restart, it resets that auto-restart status.
     *
     * @param reconciliation    Reconciliation marker
     * @param host              Kafka Connect host
     * @param apiClient         Kafka Connect REST API client
     * @param connectorName     Name of the connector
     * @param connectorSpec     Spec of the connector
     * @param status            The new/future status of the connector
     * @param resource          The custom resource used to get the existing status of the connector
     *
     * @return  Future with connector status and conditions which completes when the auto-restart is handled
     */
    @SuppressWarnings({ "rawtypes" })
    /* test */ Future<ConnectorStatusAndConditions> autoRestartFailedConnectorAndTasks(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, KafkaConnectorSpec connectorSpec, ConnectorStatusAndConditions status, CustomResource resource) {
        JsonObject statusResultJson = new JsonObject(status.statusResult);
        if (connectorSpec.getAutoRestart() != null && connectorSpec.getAutoRestart().isEnabled()) {
            return getPreviousKafkaConnectorStatus(reconciliation, connectorOperator, resource)
                 .compose(previousStatus -> {
                     if (previousStatus == null) {
                         previousStatus = new KafkaConnectorStatus();
                     }
                     return Future.succeededFuture(previousStatus.getAutoRestart());
                 })
                 .compose(previousAutoRestartStatus -> {
                     boolean needsRestart = connectorHasFailed(statusResultJson) || failedTaskIds(statusResultJson).size() > 0;

                     if (needsRestart)    {
                         // Connector or task failed and we should check it for auto-restart
                         if (shouldAutoRestart(previousAutoRestartStatus, connectorSpec.getAutoRestart().getMaxRestarts()))    {
                             // There are failures, and it is a time to restart the connector now
                             metrics().connectorsAutoRestartsCounter(reconciliation.namespace()).increment();
                             return autoRestartConnector(reconciliation, host, apiClient, connectorName, status, previousAutoRestartStatus);
                         } else {
                             // There are failures, but the next restart should happen only later => keep the original status
                             status.autoRestart = new AutoRestartStatusBuilder(previousAutoRestartStatus).build();
                             return Future.succeededFuture(status);
                         }
                     } else {
                         // Connector and tasks are not failed
                         if (previousAutoRestartStatus != null) {
                             if (shouldResetAutoRestartStatus(previousAutoRestartStatus))    {
                                 // The connector is not failing now for some time => time to reset the auto-restart status
                                 LOGGER.infoCr(reconciliation, "Resetting the auto-restart status of connector {} ", connectorName);
                                 status.autoRestart = null;
                                 return Future.succeededFuture(status);
                             } else {
                                 // The connector is not failing, but it is not sure yet if it is stable => keep the original status
                                 status.autoRestart = new AutoRestartStatusBuilder(previousAutoRestartStatus).build();
                                 return Future.succeededFuture(status);
                             }
                         } else {
                             // No failures and no need to reset the previous auto.restart state => nothing to do
                             return Future.succeededFuture(status);
                         }
                     }
                 });
        } else {
            return Future.succeededFuture(status);
        }
    }

    /**
     * Restarts the connector and updates the auto-restart status.
     *
     * @param reconciliation                Reconciliation marker
     * @param host                          Kafka Connect host
     * @param apiClient                     Kafka Connect REST API client
     * @param connectorName                 Name of the connector
     * @param status                        The new/future status of the connector
     * @param previousAutoRestartStatus     Previous auto-restart status used to generate the new status (increment the restart counter etc.)

     * @return  Future with connector status and conditions which completes when the connector is restarted
     */
    private Future<ConnectorStatusAndConditions> autoRestartConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, ConnectorStatusAndConditions status, AutoRestartStatus previousAutoRestartStatus) {
        LOGGER.infoCr(reconciliation, "Auto restarting connector {}", connectorName);
        return apiClient.restart(host, port, connectorName, true, true)
                .compose(
                        statusResult -> {
                            LOGGER.infoCr(reconciliation, "Restarted connector {} ", connectorName);
                            status.statusResult = statusResult;
                            status.autoRestart = StatusUtils.incrementAutoRestartStatus(previousAutoRestartStatus);
                            return  Future.succeededFuture(status);
                        },
                        throwable -> {
                            status.autoRestart = StatusUtils.incrementAutoRestartStatus(previousAutoRestartStatus);
                            String message = "Failed to auto restart for the " + status.autoRestart.getCount() + " time(s) connector  " + connectorName + ". " + throwable.getMessage();
                            LOGGER.warnCr(reconciliation, message);
                            return Future.succeededFuture(status);
                        });
    }

    /**
     * Checks whether it is time for another connector restart. The decision is made based on the timing provided by
     * {@code autoRestartBackOffInterval} and the value specified for the {@code maxRestarts} parameter.
     *
     * @param autoRestartStatus     Status field with auto-restart status
     * @param maxRestarts           Maximum number of restarts (or null for unlimited restarts)
     *
     * @return True if the connector should be auto-restarted right now. False otherwise.
     */
    /* test */ boolean shouldAutoRestart(AutoRestartStatus autoRestartStatus, Integer maxRestarts) {
        if (autoRestartStatus == null
                || autoRestartStatus.getLastRestartTimestamp() == null) {
            // If there is no previous auto.restart status or timestamp, we always restart it
            return true;
        } else {
            // If the status of the previous restart is present, we calculate if it is time for another restart
            var count = autoRestartStatus.getCount();
            var minutesSinceLastRestart = StatusUtils.minutesDifferenceUntilNow(StatusUtils.isoUtcDatetime(autoRestartStatus.getLastRestartTimestamp()));

            return (maxRestarts == null || count < maxRestarts)
                    && minutesSinceLastRestart >= nextAutoRestartBackOffIntervalInMinutes(count);
        }
    }

    /**
     * Calculates the back-off interval for auto-restarting the connectors. It is calculated as (n^2 + n) where n is the
     * number of previous restarts, but is capped at max 60 minutes. As a result, the restarts should be done after 0,
     * 2, 6, 12, 20, 30, 42, and 56 minutes and then every 60 minutes.
     *
     * @param restartCount  Number of restarts already applied to the connector
     *
     * @return  Number of minutes after which the next restart should happen
     */
    private int nextAutoRestartBackOffIntervalInMinutes(int restartCount)    {
        return Math.min(restartCount * restartCount + restartCount, 60);
    }

    /**
     * Checks whether the connector is stable for long enough after the previous restart to reset the auto-restart
     * counters. Normally, this follows the same backoff intervals as the restarts. For example, after 4 restarts, the
     * connector needs to be running for 20 minutes to be considered stable.
     *
     * @param autoRestartStatus     Status field with auto-restart status
     *
     * @return  True if the previous auto-restart status of the connector should be reset to 0. False otherwise.
     */
    /* test */ boolean shouldResetAutoRestartStatus(AutoRestartStatus autoRestartStatus) {
        if (autoRestartStatus != null
                && autoRestartStatus.getLastRestartTimestamp() != null
                && autoRestartStatus.getCount() > 0) {
            // There are previous auto-restarts => we check if it is time to reset the status
            long minutesSinceLastRestart = StatusUtils.minutesDifferenceUntilNow(StatusUtils.isoUtcDatetime(autoRestartStatus.getLastRestartTimestamp()));

            return minutesSinceLastRestart > nextAutoRestartBackOffIntervalInMinutes(autoRestartStatus.getCount());
        } else {
            // There are no previous restarts => nothing to reset
            return false;
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private Future<List<Condition>> maybeRestartConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, CustomResource resource, List<Condition> conditions) {
        if (hasRestartAnnotation(resource, connectorName)) {
            LOGGER.debugCr(reconciliation, "Restarting connector {}", connectorName);
            return apiClient.restart(host, port, connectorName, false, false)
                    .compose(ignored -> removeRestartAnnotation(reconciliation, resource)
                        .compose(v -> Future.succeededFuture(conditions)),
                        throwable -> {
                            // Ignore restart failures - add a warning and try again on the next reconcile
                            String message = "Failed to restart connector " + connectorName + ". " + throwable.getMessage();
                            LOGGER.warnCr(reconciliation, message);
                            conditions.add(StatusUtils.buildWarningCondition("RestartConnector", message));
                            return Future.succeededFuture(conditions);
                        });
        } else {
            return Future.succeededFuture(conditions);
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private Future<List<Condition>> maybeRestartConnectorTask(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, CustomResource resource, List<Condition> conditions) {
        int taskID = getRestartTaskAnnotationTaskID(resource, connectorName);
        if (taskID >= 0) {
            LOGGER.debugCr(reconciliation, "Restarting connector task {}:{}", connectorName, taskID);
            return apiClient.restartTask(host, port, connectorName, taskID)
                    .compose(ignored -> removeRestartTaskAnnotation(reconciliation, resource)
                        .compose(v -> Future.succeededFuture(conditions)),
                        throwable -> {
                            // Ignore restart failures - add a warning and try again on the next reconcile
                            String message = "Failed to restart connector task " + connectorName + ":" + taskID + ". " + throwable.getMessage();
                            LOGGER.warnCr(reconciliation, message);
                            conditions.add(StatusUtils.buildWarningCondition("RestartConnectorTask", message));
                            return Future.succeededFuture(conditions);
                        });
        } else {
            return Future.succeededFuture(conditions);
        }
    }

    private Future<ConnectorStatusAndConditions> updateConnectorTopics(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, ConnectorStatusAndConditions status) {
        return apiClient.getConnectorTopics(reconciliation, host, port, connectorName)
            .compose(updateConnectorStatusAndConditions(status));
    }

    /**
     * Whether the provided resource instance is a KafkaConnector and has the strimzi.io/restart annotation
     *
     * @param resource resource instance to check
     * @param connectorName connectorName name of the connector to check
     * @return true if the provided resource instance has the strimzi.io/restart annotation; false otherwise
     */
    @SuppressWarnings({ "rawtypes" })
    protected boolean hasRestartAnnotation(CustomResource resource, String connectorName) {
        return Annotations.booleanAnnotation(resource, ANNO_STRIMZI_IO_RESTART, false);
    }

    /**
     * Return the ID of the connector task to be restarted if the provided KafkaConnector resource instance has the strimzi.io/restart-task annotation
     *
     * @param resource resource instance to check
     * @param connectorName KafkaConnector resource instance to check
     * @return the ID of the task to be restarted if the provided KafkaConnector resource instance has the strimzi.io/restart-task annotation or -1 otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    protected int getRestartTaskAnnotationTaskID(CustomResource resource, String connectorName) {
        return Annotations.intAnnotation(resource, ANNO_STRIMZI_IO_RESTART_TASK, -1);
    }

    /**
     * Patches the KafkaConnector CR to remove the strimzi.io/restart annotation, as
     * the restart action specified by the user has been completed.
     */
    @SuppressWarnings({ "rawtypes" })
    protected Future<Void> removeRestartAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_RESTART);
    }

    /**
     * Patches the KafkaConnector CR to remove the strimzi.io/restart-task annotation, as
     * the restart action specified by the user has been completed.
     */
    @SuppressWarnings({ "rawtypes" })
    protected Future<Void> removeRestartTaskAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_RESTART_TASK);
    }

    /**
     * Patches the KafkaConnector CR to remove the supplied annotation.
     */
    private Future<Void> removeAnnotation(Reconciliation reconciliation, KafkaConnector resource, String annotationKey) {
        LOGGER.debugCr(reconciliation, "Removing annotation {}", annotationKey);
        KafkaConnector patchedKafkaConnector = new KafkaConnectorBuilder(resource)
            .editMetadata()
            .removeFromAnnotations(annotationKey)
            .endMetadata()
            .build();
        return connectorOperator.patchAsync(reconciliation, patchedKafkaConnector)
            .compose(ignored -> Future.succeededFuture());
    }

    /**
     * Update the status of the Kafka Connector custom resource
     *
     * @param reconciliation        Reconciliation marker
     * @param error                 Throwable indicating if any errors occurred during the reconciliation
     * @param kafkaConnector2       Latest version of the KafkaConnector resource where the status should be updated
     * @param connectorOperations   The KafkaConnector operations for updating the status
     */
    public static void updateStatus(Reconciliation reconciliation, Throwable error, KafkaConnector kafkaConnector2, CrdOperator<?, KafkaConnector, ?> connectorOperations) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnector2, status, error);
        StatusDiff diff = new StatusDiff(kafkaConnector2.getStatus(), status);
        if (!diff.isEmpty()) {
            KafkaConnector copy = new KafkaConnectorBuilder(kafkaConnector2).build();
            copy.setStatus(status);
            connectorOperations.updateStatusAsync(reconciliation, copy);
        }
    }

    static protected class ConnectorStatusAndConditions {
        Map<String, Object> statusResult;
        List<String> topics;
        List<Condition> conditions;
        AutoRestartStatus autoRestart;

        ConnectorStatusAndConditions(Map<String, Object> statusResult, List<String> topics, List<Condition> conditions, AutoRestartStatus autoRestart) {
            this.statusResult = statusResult;
            this.topics = topics;
            this.conditions = conditions;
            this.autoRestart = autoRestart;
        }

        ConnectorStatusAndConditions(Map<String, Object> statusResult, List<Condition> conditions) {
            this(statusResult, Collections.emptyList(), conditions, null);
        }

        ConnectorStatusAndConditions(Map<String, Object> statusResult) {
            this(statusResult, Collections.emptyList(), Collections.emptyList(), null);
        }
    }

    Function<Map<String, Object>, Future<ConnectorStatusAndConditions>> createConnectorStatusAndConditions() {
        return statusResult -> Future.succeededFuture(new ConnectorStatusAndConditions(statusResult));
    }

    Function<Map<String, Object>, Future<ConnectorStatusAndConditions>> createConnectorStatusAndConditions(List<Condition> conditions) {
        return statusResult -> Future.succeededFuture(new ConnectorStatusAndConditions(statusResult, conditions));
    }

    Function<List<String>, Future<ConnectorStatusAndConditions>> updateConnectorStatusAndConditions(ConnectorStatusAndConditions status) {
        return topics -> Future.succeededFuture(new ConnectorStatusAndConditions(status.statusResult, topics, status.conditions, status.autoRestart));
    }

    /**
     * Validates the KafkaConnector resource and returns any warning conditions find during the validation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaConnector resource which should be validated
     *
     * @return  Set with the warning conditions. Empty set if no conditions were found.
     */
    public Set<Condition> validate(Reconciliation reconciliation, KafkaConnector resource) {
        if (resource != null) {
            return StatusUtils.validate(reconciliation, resource);
        }

        return Collections.emptySet();
    }

    Future<Void> maybeUpdateConnectorStatus(Reconciliation reconciliation, KafkaConnector connector, ConnectorStatusAndConditions connectorStatus, Throwable error) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        if (error != null) {
            LOGGER.warnCr(reconciliation, "Error reconciling connector {}", connector.getMetadata().getName(), error);
        }

        Map<String, Object> statusResult = null;
        List<String> topics = new ArrayList<>();
        List<Condition> conditions = new ArrayList<>();
        AutoRestartStatus autoRestart = null;

        Future<Void> connectorReadiness = Future.succeededFuture();

        if (connectorStatus != null) {
            statusResult = connectorStatus.statusResult;
            JsonObject statusResultJson = new JsonObject(statusResult);
            List<String> failedTaskIds;
            if (connectorHasFailed(statusResultJson)) {
                connectorReadiness = Future.failedFuture(new Throwable("Connector has failed, see connectorStatus for more details."));
            } else if ((failedTaskIds = failedTaskIds(statusResultJson)).size() > 0) {
                connectorReadiness = Future.failedFuture(new Throwable(String.format("The following tasks have failed: %s, see connectorStatus for more details.", String.join(", ", failedTaskIds))));
            }
            topics = connectorStatus.topics.stream().sorted().collect(Collectors.toList());
            conditions.addAll(connectorStatus.conditions);
            autoRestart = connectorStatus.autoRestart;
        }

        Set<Condition> unknownAndDeprecatedConditions = validate(reconciliation, connector);
        conditions.addAll(unknownAndDeprecatedConditions);

        if (!Annotations.isReconciliationPausedWithAnnotation(connector)) {
            StatusUtils.setStatusConditionAndObservedGeneration(connector, status, error != null ? error : connectorReadiness.cause());
            status.setConnectorStatus(statusResult);
            status.setTasksMax(getActualTaskCount(connector, statusResult));
            status.setTopics(topics);
            status.setAutoRestart(autoRestart);
        } else {
            status.setObservedGeneration(connector.getStatus() != null ? connector.getStatus().getObservedGeneration() : 0);
            conditions.add(StatusUtils.getPausedCondition());
        }
        status.addConditions(conditions);

        return maybeUpdateStatusCommon(connectorOperator, connector, reconciliation, status,
            (connector1, status1) -> {
                return new KafkaConnectorBuilder(connector1).withStatus(status1).build();
            });
    }

    private boolean connectorHasFailed(JsonObject statusResult) {
        JsonObject connectorStatus = statusResult.getJsonObject("connector");
        return connectorStatus != null && "FAILED".equals(connectorStatus.getString("state"));
    }

    private List<String> failedTaskIds(JsonObject statusResult) {
        JsonArray tasks = Optional.ofNullable(statusResult.getJsonArray("tasks")).orElse(new JsonArray());
        List<String> failedTasks = new ArrayList<>();
        for (Object task : tasks) {
            if (task instanceof JsonObject && "FAILED".equals(((JsonObject) task).getString("state"))) {
                failedTasks.add(((JsonObject) task).getString("id"));
            }
        }
        return failedTasks;
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
    @SuppressWarnings({ "rawtypes" })
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

    protected JsonObject asJson(KafkaConnectorSpec spec, KafkaConnectorConfiguration desiredConfig) {
        JsonObject connectorConfigJson = new JsonObject();

        for (Map.Entry<String, String> cf : desiredConfig.asOrderedProperties().asMap().entrySet()) {
            connectorConfigJson.put(cf.getKey(), cf.getValue());
        }

        if (spec.getTasksMax() != null) {
            connectorConfigJson.put("tasks.max", spec.getTasksMax());
        }

        return connectorConfigJson.put("connector.class", spec.getClassName());
    }

    @SuppressWarnings({ "rawtypes" })
    protected Future<KafkaConnectorStatus> getPreviousKafkaConnectorStatus(Reconciliation reconciliation, CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> resourceOperator,
                                                           CustomResource resource) {
        return resourceOperator
            .getAsync(resource.getMetadata().getNamespace(), resource.getMetadata().getName())
            .compose(result -> {
                if (result != null) {
                    return Future.succeededFuture(result.getStatus());
                } else {
                    // This is unexpected, since we are reconciling the resource and therefore it should exist. So the
                    // result being null suggests some race condition. We log a warning and return null which is handled
                    // in the place calling this method.
                    LOGGER.warnCr(reconciliation, "Previous Kafka Connector status was not found");
                    return Future.succeededFuture(null);
                }
            });
    }

    /**
     * Updates the Status field of the KafkaConnect or KafkaConnector CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param resourceOperator  The resource operator for managing custom resources
     * @param resource          The CR of KafkaConnect or KafkaConnector
     * @param reconciliation    Reconciliation information
     * @param desiredStatus     The KafkaConnectStatus or KafkaConnectorStatus which should be set
     * @param copyWithStatus    BiFunction for copying parts of the status
     *
     * @param <T>   Custom resource type
     * @param <S>   Custom resource status type
     * @param <L>   Custom resource list type
     *
     * @return  Future which completes then the status is updated
     */
    protected <T extends CustomResource<?, S>, S extends Status, L extends DefaultKubernetesResourceList<T>> Future<Void>
        maybeUpdateStatusCommon(CrdOperator<KubernetesClient, T, L> resourceOperator,
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
                            && (!(fetchedResource instanceof KafkaMirrorMaker2))) {
                        LOGGER.warnCr(reconciliation, "{} {} needs to be upgraded from version {} to 'v1beta1' to use the status field",
                                fetchedResource.getKind(), fetchedResource.getMetadata().getName(), fetchedResource.getApiVersion());
                        updateStatusPromise.complete();
                    } else {
                        S currentStatus = fetchedResource.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            T resourceWithNewStatus = copyWithStatus.apply(fetchedResource, desiredStatus);

                            resourceOperator.updateStatusAsync(reconciliation, resourceWithNewStatus).onComplete(updateRes -> {
                                if (updateRes.succeeded()) {
                                    LOGGER.debugCr(reconciliation, "Completed status update");
                                    updateStatusPromise.complete();
                                } else {
                                    LOGGER.errorCr(reconciliation, "Failed to update status", updateRes.cause());
                                    updateStatusPromise.fail(updateRes.cause());
                                }
                            });
                        } else {
                            LOGGER.debugCr(reconciliation, "Status did not change");
                            updateStatusPromise.complete();
                        }
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Current {} resource not found", resource.getKind());
                    updateStatusPromise.fail("Current " + resource.getKind() + " resource not found");
                }
            } else {
                LOGGER.errorCr(reconciliation, "Failed to get the current {} resource and its status", resource.getKind(), getRes.cause());
                updateStatusPromise.fail(getRes.cause());
            }
        });

        return updateStatusPromise.future();
    }

    /**
     * Creates the ClusterRoleBinding required for the init container used for client rack-awareness.
     * The init-container needs to be able to read the labels from the node it is running on to be able
     * to determine the `client.rack` option.
     *
     * @param reconciliation The reconciliation
     * @param crbName ClusterRoleBinding name
     * @param crb ClusterRoleBinding
     *
     * @return Future for tracking the asynchronous result of the ClusterRoleBinding reconciliation
     */
    protected Future<ReconcileResult<ClusterRoleBinding>> connectInitClusterRoleBinding(Reconciliation reconciliation, String crbName, ClusterRoleBinding crb) {
        return ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, crbName, crb), crb);
    }
}
