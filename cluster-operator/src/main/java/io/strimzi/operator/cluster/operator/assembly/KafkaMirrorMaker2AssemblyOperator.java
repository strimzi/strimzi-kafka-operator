/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK;
import static java.util.Collections.emptyMap;

/**
 * <p>Assembly operator for a "Kafka MirrorMaker 2" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>A set of MirrorMaker 2 connectors</li>
 * </ul>
 */
public class KafkaMirrorMaker2AssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List, KafkaMirrorMaker2Spec, KafkaMirrorMaker2Status> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaMirrorMaker2AssemblyOperator.class.getName());

    /**
     * Constructor
     *
     * @param vertx     The Vertx instance
     * @param pfa       Platform features availability properties
     * @param supplier  Supplies the operators for different resources
     * @param config    ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaMirrorMaker2AssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    /**
     * Constructor used for tests
     *
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     * @param connectClientProvider     Connect REST APi client provider
     */
    protected KafkaMirrorMaker2AssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, KafkaMirrorMaker2.RESOURCE_KIND, supplier.mirrorMaker2Operator, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
    }

    @Override
    protected Future<KafkaMirrorMaker2Status> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster;
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();
        try {
            mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(reconciliation, kafkaMirrorMaker2, versions, sharedEnvironmentProvider);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaMirrorMaker2, kafkaMirrorMaker2Status, e);
            return Future.failedFuture(new ReconciliationException(kafkaMirrorMaker2Status, e));
        }

        Promise<KafkaMirrorMaker2Status> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();

        Map<String, String> podAnnotations = new HashMap<>(1);

        final AtomicReference<String> desiredLogging = new AtomicReference<>();
        final AtomicReference<Deployment> deployment = new AtomicReference<>();
        final AtomicReference<StrimziPodSet> podSet = new AtomicReference<>();

        boolean hasZeroReplicas = mirrorMaker2Cluster.getReplicas() == 0;
        String initCrbName = KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(kafkaMirrorMaker2.getMetadata().getName(), namespace);
        ClusterRoleBinding initCrb = mirrorMaker2Cluster.generateClusterRoleBinding();

        LOGGER.debugCr(reconciliation, "Updating Kafka MirrorMaker 2 cluster");

        controllerResources(reconciliation, mirrorMaker2Cluster, deployment, podSet)
                .compose(i -> connectServiceAccount(reconciliation, namespace, KafkaMirrorMaker2Resources.serviceAccountName(mirrorMaker2Cluster.getCluster()), mirrorMaker2Cluster))
                .compose(i -> connectInitClusterRoleBinding(reconciliation, initCrbName, initCrb))
                .compose(i -> connectNetworkPolicy(reconciliation, namespace, mirrorMaker2Cluster, true))
                .compose(i -> manualRollingUpdate(reconciliation, mirrorMaker2Cluster))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, mirrorMaker2Cluster.getServiceName(), mirrorMaker2Cluster.generateService()))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, mirrorMaker2Cluster.getComponentName(), mirrorMaker2Cluster.generateHeadlessService()))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, mirrorMaker2Cluster))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(mirrorMaker2Cluster.logging().configMapKey());
                    podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    desiredLogging.set(logging);
                    return configMapOperations.reconcile(reconciliation, namespace, logAndMetricsConfigMap.getMetadata().getName(), logAndMetricsConfigMap);
                })
                .compose(i -> ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperations, mirrorMaker2Cluster))
                .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, mirrorMaker2Cluster.getComponentName(), mirrorMaker2Cluster.generatePodDisruptionBudget()))
                .compose(i -> generateAuthHash(namespace, kafkaMirrorMaker2.getSpec()))
                .compose(hash -> {
                    podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    return Future.succeededFuture();
                })
                .compose(i -> new KafkaConnectMigration(
                                reconciliation,
                                mirrorMaker2Cluster,
                                null,
                                podAnnotations,
                                operationTimeoutMs,
                                pfa.isOpenshift(),
                                imagePullPolicy,
                                imagePullSecrets,
                                null,
                                deploymentOperations,
                                podSetOperations,
                                podOperations
                        )
                        .migrateFromDeploymentToStrimziPodSets(deployment.get(), podSet.get()))
                .compose(i -> reconcilePodSet(reconciliation, mirrorMaker2Cluster, podAnnotations))
                .compose(i -> hasZeroReplicas ? Future.succeededFuture() : reconcileConnectors(reconciliation, kafkaMirrorMaker2, mirrorMaker2Cluster, kafkaMirrorMaker2Status, desiredLogging.get()))
                .map((Void) null)
                .onComplete(reconciliationResult -> {
                    List<Condition> conditions = kafkaMirrorMaker2Status.getConditions();
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaMirrorMaker2, kafkaMirrorMaker2Status, reconciliationResult.cause());

                    if (!hasZeroReplicas) {
                        kafkaMirrorMaker2Status.setUrl(KafkaMirrorMaker2Resources.url(mirrorMaker2Cluster.getCluster(), namespace, KafkaMirrorMaker2Cluster.REST_API_PORT));
                    }
                    if (conditions != null && !conditions.isEmpty()) {
                        kafkaMirrorMaker2Status.addConditions(conditions);
                    }
                    kafkaMirrorMaker2Status.setReplicas(mirrorMaker2Cluster.getReplicas());
                    kafkaMirrorMaker2Status.setLabelSelector(mirrorMaker2Cluster.getSelectorLabels().toSelectorString());

                    if (reconciliationResult.succeeded())   {
                        createOrUpdatePromise.complete(kafkaMirrorMaker2Status);
                    } else {
                        createOrUpdatePromise.fail(new ReconciliationException(kafkaMirrorMaker2Status, reconciliationResult.cause()));
                    }
                });

        return createOrUpdatePromise.future();
    }

    private Future<Void> controllerResources(Reconciliation reconciliation,
                                             KafkaMirrorMaker2Cluster mirrorMaker2Cluster,
                                             AtomicReference<Deployment> deploymentReference,
                                             AtomicReference<StrimziPodSet> podSetReference)   {
        return Future
                .join(deploymentOperations.getAsync(reconciliation.namespace(), mirrorMaker2Cluster.getComponentName()), podSetOperations.getAsync(reconciliation.namespace(), mirrorMaker2Cluster.getComponentName()))
                .compose(res -> {
                    deploymentReference.set(res.resultAt(0));
                    podSetReference.set(res.resultAt(1));

                    return Future.succeededFuture();
                });
    }

    private Future<Void> reconcilePodSet(Reconciliation reconciliation,
                                         KafkaConnectCluster connect,
                                         Map<String, String> podAnnotations)  {
        return podSetOperations.reconcile(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.generatePodSet(connect.getReplicas(), null, podAnnotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, null))
                .compose(reconciliationResult -> {
                    KafkaConnectRoller roller = new KafkaConnectRoller(reconciliation, connect, operationTimeoutMs, podOperations);
                    return roller.maybeRoll(PodSetUtils.podNames(reconciliationResult.resource()), pod -> KafkaConnectRoller.needsRollingRestart(reconciliationResult.resource(), pod));
                })
                .compose(i -> podSetOperations.readiness(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs));
    }

    @Override
    protected KafkaMirrorMaker2Status createStatus(KafkaMirrorMaker2 ignored) {
        return new KafkaMirrorMaker2Status();
    }

    /**
     * Generates a hash from the trusted TLS certificates that can be used to spot if it has changed.
     *
     * @param namespace               Namespace of the MirrorMaker2 cluster
     * @param kafkaMirrorMaker2Spec   KafkaMirrorMaker2Spec object
     * @return                        Future for tracking the asynchronous result of generating the TLS auth hash
     */
    private Future<Integer> generateAuthHash(String namespace, KafkaMirrorMaker2Spec kafkaMirrorMaker2Spec) {
        Promise<Integer> authHash = Promise.promise();
        if (kafkaMirrorMaker2Spec.getClusters() == null) {
            authHash.complete(0);
        } else {
            Future.join(kafkaMirrorMaker2Spec.getClusters()
                            .stream()
                            .map(cluster -> {
                                List<CertSecretSource> trustedCertificates = cluster.getTls() == null ? Collections.emptyList() : cluster.getTls().getTrustedCertificates();
                                return VertxUtil.authTlsHash(secretOperations, namespace, cluster.getAuthentication(), trustedCertificates);
                            }).collect(Collectors.toList())
                    )
                    .onSuccess(hashes -> {
                        int hash = hashes.<Integer>list()
                            .stream()
                            .mapToInt(i -> i)
                            .sum();
                        authHash.complete(hash);
                    }).onFailure(authHash::fail);
        }
        return authHash.future();
    }

    /**
     * Reconcile all the MirrorMaker 2 connectors selected by the given MirrorMaker 2 instance.
     * @param reconciliation The reconciliation
     * @param kafkaMirrorMaker2 The MirrorMaker 2
     * @return A future, failed if any of the connectors could not be reconciled.
     */
    private Future<Void> reconcileConnectors(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Cluster mirrorMaker2Cluster, KafkaMirrorMaker2Status mirrorMaker2Status, String desiredLogging) {
        String host = KafkaMirrorMaker2Resources.qualifiedServiceName(mirrorMaker2Cluster.getCluster(), reconciliation.namespace());
        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);
        List<KafkaConnector> desiredConnectors = mirrorMaker2Cluster.connectors().generateConnectorDefinitions();

        return apiClient.list(reconciliation, host, KafkaConnectCluster.REST_API_PORT).compose(currentConnectors -> {
            currentConnectors.removeAll(desiredConnectors.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));

            Future<Void> deletionFuture = deleteConnectors(reconciliation, host, apiClient, currentConnectors);
            Future<Void> createOrUpdateFuture = reconcileConnectors(reconciliation, host, apiClient, kafkaMirrorMaker2, mirrorMaker2Cluster, desiredConnectors, mirrorMaker2Status, desiredLogging);

            return Future.join(deletionFuture, createOrUpdateFuture).map((Void) null);
        });
    }

    private static Future<Void> deleteConnectors(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, List<String> connectorsForDeletion) {
        return Future.join(connectorsForDeletion.stream()
                        .map(connectorName -> {
                            LOGGER.debugCr(reconciliation, "Deleting connector {}", connectorName);
                            return apiClient.delete(reconciliation, host, KafkaConnectCluster.REST_API_PORT, connectorName);
                        })
                        .collect(Collectors.toList()))
                .map((Void) null);
    }

    private Future<Void> reconcileConnectors(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, KafkaMirrorMaker2 mirrorMaker2, KafkaMirrorMaker2Cluster mirrorMaker2Cluster, List<KafkaConnector> connectors, KafkaMirrorMaker2Status mirrorMaker2Status, String desiredLogging) {
        return Future.join(connectors.stream()
                        .map(connector -> {
                            LOGGER.debugCr(reconciliation, "Creating / updating connector {}", connector.getMetadata().getName());
                            return reconcileMirrorMaker2Connector(reconciliation, mirrorMaker2, apiClient, host, connector.getMetadata().getName(), connector.getSpec(), mirrorMaker2Status);
                        })
                        .collect(Collectors.toList()))
                .map((Void) null)
                .compose(i -> apiClient.updateConnectLoggers(reconciliation, host, KafkaConnectCluster.REST_API_PORT, desiredLogging, mirrorMaker2Cluster.defaultLogConfig()))
                .compose(i -> {
                    boolean failedConnector = mirrorMaker2Status.getConnectors().stream()
                            .anyMatch(connector -> {
                                @SuppressWarnings({ "rawtypes" })
                                Object state = ((Map) connector.getOrDefault("connector", emptyMap())).get("state");
                                return "FAILED".equalsIgnoreCase(state.toString());
                            });
                    if (failedConnector) {
                        return Future.failedFuture("One or more connectors are in FAILED state");
                    } else {
                        return Future.succeededFuture();
                    }
                })
                .map((Void) null);
    }

    private Future<Void> reconcileMirrorMaker2Connector(Reconciliation reconciliation, KafkaMirrorMaker2 mirrorMaker2, KafkaConnectApi apiClient, String host, String connectorName, KafkaConnectorSpec connectorSpec, KafkaMirrorMaker2Status mirrorMaker2Status) {
        return maybeCreateOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec, mirrorMaker2)
                .onComplete(result -> {
                    if (result.succeeded()) {
                        mirrorMaker2Status.addConditions(result.result().conditions);
                        mirrorMaker2Status.getConnectors().add(result.result().statusResult);
                        mirrorMaker2Status.getConnectors().sort(new ConnectorsComparatorByName());
                        var autoRestart = result.result().autoRestart;
                        if (autoRestart != null) {
                            autoRestart.setConnectorName(connectorName);
                            mirrorMaker2Status.getAutoRestartStatuses().add(autoRestart);
                            mirrorMaker2Status.getAutoRestartStatuses().sort(Comparator.comparing(AutoRestartStatus::getConnectorName));
                        }
                    } else {
                        maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2, result.cause());
                    }
                }).compose(ignored -> Future.succeededFuture());
    }

    private Future<Void> maybeUpdateMirrorMaker2Status(Reconciliation reconciliation, KafkaMirrorMaker2 mirrorMaker2, Throwable error) {
        KafkaMirrorMaker2Status status = new KafkaMirrorMaker2Status();
        if (error != null) {
            LOGGER.warnCr(reconciliation, "Error reconciling MirrorMaker 2 {}", mirrorMaker2.getMetadata().getName(), error);
        }
        StatusUtils.setStatusConditionAndObservedGeneration(mirrorMaker2, status, error);
        return maybeUpdateStatusCommon(resourceOperator, mirrorMaker2, reconciliation, status,
            (mirror1, status2) -> new KafkaMirrorMaker2Builder(mirror1).withStatus(status2).build());
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null)
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }

    // Methods for working with restart annotations

    /**
     * Checks whether the provided KafkaMirrorMaker2 resource has the strimzi.io/restart-connector annotation, and
     * whether it's value matches the supplied connectorName
     *
     * @param resource          KafkaMirrorMaker2 resource instance to check
     * @param connectorName     Connector name of the MM2 connector to check
     *
     * @return  True if the provided resource instance has the strimzi.io/restart-connector annotation. False otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected boolean hasRestartAnnotation(CustomResource resource, String connectorName) {
        String restartAnnotationConnectorName = Annotations.stringAnnotation(resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR, null);
        return connectorName.equals(restartAnnotationConnectorName);
    }

    /**
     * Return the ID of the connector task to be restarted if the provided resource instance has the strimzi.io/restart-connector-task annotation
     *
     * @param resource          KafkaMirrorMaker2 resource instance to check
     * @param connectorName     Connector name of the MM2 connector to check
     *
     * @return  The ID of the task to be restarted if the provided KafkaMirrorMaker2 resource instance has the strimzi.io/restart-connector-task annotation or -1 otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected int getRestartTaskAnnotationTaskID(CustomResource resource, String connectorName) {
        int taskID = -1;
        String connectorTask = Annotations.stringAnnotation(resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK, "").trim();
        Matcher connectorTaskMatcher = ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN.matcher(connectorTask);
        if (connectorTaskMatcher.matches() && connectorTaskMatcher.group(ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR).equals(connectorName)) {
            taskID = Integer.parseInt(connectorTaskMatcher.group(ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK));
        }
        return taskID;
    }

    /**
     * Patches the custom resource to remove the restart annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaMirrorMaker2 resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected Future<Void> removeRestartAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaMirrorMaker2) resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR);
    }


    /**
     * Patches the custom resource to remove the restart task annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaMirrorMaker2 resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected Future<Void> removeRestartTaskAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaMirrorMaker2) resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK);
    }

    /**
     * Patches the KafkaMirrorMaker2 CR to remove the supplied annotation.
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaMirrorMaker2 resource from which the annotation should be removed
     * @param annotationKey     Annotation that should be removed
     *
     * @return  Future that indicates the operation completion
     */
    private Future<Void> removeAnnotation(Reconciliation reconciliation, KafkaMirrorMaker2 resource, String annotationKey) {
        LOGGER.debugCr(reconciliation, "Removing annotation {}", annotationKey);
        KafkaMirrorMaker2 patchedKafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(resource)
                .editMetadata()
                .removeFromAnnotations(annotationKey)
                .endMetadata()
                .build();
        return resourceOperator.patchAsync(reconciliation, patchedKafkaMirrorMaker2)
                .compose(ignored -> Future.succeededFuture());
    }

    // Static utility methods and classes

    /**
     * This comparator compares two maps where connectors' configurations are stored.
     * The comparison is done by using only one property - 'name'
     */
    static class ConnectorsComparatorByName implements Comparator<Map<String, Object>>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Map<String, Object> m1, Map<String, Object> m2) {
            String name1 = m1.get("name") == null ? "" : m1.get("name").toString();
            String name2 = m2.get("name") == null ? "" : m2.get("name").toString();
            return name1.compareTo(name2);
        }
    }
}
