/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.connect.AbstractKafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatusBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
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
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Abstract operator for managing Connect based resources (Connect and Mirror Maker 2)
 *
 * @param <C>   Kubernetes client type
 * @param <T>   Custom Resource type
 * @param <L>   Custom Resource List type
 * @param <P>   Custom Resource Spec type
 * @param <S>   Custom Resource Status type
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public abstract class AbstractConnectOperator<C extends KubernetesClient, T extends CustomResource<P, S>,
        L extends DefaultKubernetesResourceList<T>, P extends AbstractKafkaConnectSpec, S extends KafkaConnectStatus>
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
    protected final int port;

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
     * @param reconciliation        The reconciliation
     * @param kafkaConnectCluster   KafkaConnectCluster object
     *
     * @return Future for tracking the asynchronous result of getting the metrics and logging config map
     */
    protected Future<ConfigMap> generateMetricsAndLoggingConfigMap(Reconciliation reconciliation, KafkaConnectCluster kafkaConnectCluster) {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperations, kafkaConnectCluster.logging(), kafkaConnectCluster.metrics())
                .compose(metricsAndLoggingCm -> Future.succeededFuture(kafkaConnectCluster.generateMetricsAndLogConfigMap(metricsAndLoggingCm)));
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

    private Future<Map<String, Object>> createOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
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
                     boolean needsRestart = connectorHasFailed(statusResultJson) || !failedTaskIds(statusResultJson).isEmpty();

                     if (needsRestart)    {
                         // Connector or task failed, and we should check it for auto-restart
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
    private static int nextAutoRestartBackOffIntervalInMinutes(int restartCount)    {
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
    /* test */ static boolean shouldResetAutoRestartStatus(AutoRestartStatus autoRestartStatus) {
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

    // Abstract methods for working with restart annotations. These methods are implemented in the KafkaConnectAssemblyOperator and KafkaMirrorMaker2AssemblyOperator

    /**
     * Checks whether the provided resource instance (a KafkaConnector or KafkaMirrorMaker2) has the annotation requesting a restart.
     *
     * @param resource          Resource instance to check
     * @param connectorName     Connector name of the connector to check
     *
     * @return True if the provided resource has the restart annotation. False otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    abstract boolean hasRestartAnnotation(CustomResource resource, String connectorName);

    /**
     * Returns the ID of the connector task to be restarted from the (a KafkaConnector or KafkaMirrorMaker2) custom resource.
     *
     * @param resource          Resource instance to check
     * @param connectorName     Connector name to check
     *
     * @return  The ID of the task to be restarted based on the annotation or -1 otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    abstract int getRestartTaskAnnotationTaskID(CustomResource resource, String connectorName);

    /**
     * Patches the custom resource to remove the restart annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Custom resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    abstract Future<Void> removeRestartAnnotation(Reconciliation reconciliation, CustomResource resource);

    /**
     * Patches the custom resource to remove the restart task annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Custom resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    abstract Future<Void> removeRestartTaskAnnotation(Reconciliation reconciliation, CustomResource resource);

    // Static utility methods and classes

    private static Function<Map<String, Object>, Future<ConnectorStatusAndConditions>> createConnectorStatusAndConditions() {
        return statusResult -> Future.succeededFuture(new ConnectorStatusAndConditions(statusResult, List.of(), List.of(), null));
    }

    private static Function<Map<String, Object>, Future<ConnectorStatusAndConditions>> createConnectorStatusAndConditions(List<Condition> conditions) {
        return statusResult -> Future.succeededFuture(new ConnectorStatusAndConditions(statusResult, List.of(), conditions, null));
    }

    private static Function<List<String>, Future<ConnectorStatusAndConditions>> updateConnectorStatusAndConditions(ConnectorStatusAndConditions status) {
        return topics -> Future.succeededFuture(new ConnectorStatusAndConditions(status.statusResult, topics, status.conditions, status.autoRestart));
    }

    protected static boolean connectorHasFailed(JsonObject statusResult) {
        JsonObject connectorStatus = statusResult.getJsonObject("connector");
        return connectorStatus != null && "FAILED".equals(connectorStatus.getString("state"));
    }

    protected static List<String> failedTaskIds(JsonObject statusResult) {
        JsonArray tasks = Optional.ofNullable(statusResult.getJsonArray("tasks")).orElse(new JsonArray());
        List<String> failedTasks = new ArrayList<>();
        for (Object task : tasks) {
            if (task instanceof JsonObject && "FAILED".equals(((JsonObject) task).getString("state"))) {
                failedTasks.add(((JsonObject) task).getString("id"));
            }
        }
        return failedTasks;
    }

    private static JsonObject asJson(KafkaConnectorSpec spec, KafkaConnectorConfiguration desiredConfig) {
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
    private static Future<KafkaConnectorStatus> getPreviousKafkaConnectorStatus(Reconciliation reconciliation, CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> resourceOperator,
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
     * Updates the Status field of the KafkaConnect or KafkaConnector CR. It diffs the desired status against the
     * current status and calls the update only when there is any difference in non-timestamp fields. The parameters
     * defined separately from the class-level parameters because they cover also KafkaConnector resources and not only
     * KafkaConnect and KafkaMirrorMaker2 resources.
     *
     * @param resourceOperator  The resource operator for managing custom resources
     * @param resource          The CR of KafkaConnect or KafkaConnector
     * @param reconciliation    Reconciliation information
     * @param desiredStatus     The KafkaConnectStatus or KafkaConnectorStatus which should be set
     * @param copyWithStatus    BiFunction for copying parts of the status
     *
     * @param <U>   Custom resource type
     * @param <V>   Custom resource status type
     * @param <W>   Custom resource list type
     *
     * @return  Future which completes then the status is updated
     */
    protected static <U extends CustomResource<?, V>, V extends Status, W extends DefaultKubernetesResourceList<U>> Future<Void>
        maybeUpdateStatusCommon(CrdOperator<KubernetesClient, U, W> resourceOperator,
                                U resource,
                                Reconciliation reconciliation,
                                V desiredStatus,
                                BiFunction<U, V, U> copyWithStatus) {
        Promise<Void> updateStatusPromise = Promise.promise();

        resourceOperator.getAsync(resource.getMetadata().getNamespace(), resource.getMetadata().getName()).onComplete(getRes -> {
            if (getRes.succeeded()) {
                U fetchedResource = getRes.result();

                if (fetchedResource != null) {
                    if ((!(fetchedResource instanceof KafkaConnector))
                            && (!(fetchedResource instanceof KafkaMirrorMaker2))) {
                        LOGGER.warnCr(reconciliation, "{} {} needs to be upgraded from version {} to 'v1beta1' to use the status field",
                                fetchedResource.getKind(), fetchedResource.getMetadata().getName(), fetchedResource.getApiVersion());
                        updateStatusPromise.complete();
                    } else {
                        V currentStatus = fetchedResource.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            U resourceWithNewStatus = copyWithStatus.apply(fetchedResource, desiredStatus);

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
     * Utility class used to transfer data. It is not immutable, so record cannot be used.
     */
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
    }
}
