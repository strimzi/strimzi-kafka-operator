/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.cluster.operator.resource.SharedEnvironmentProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.VertxUtil;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, Resource<KafkaConnect>, KafkaConnectSpec, KafkaConnectStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectAssemblyOperator.class.getName());
    private final DeploymentOperator deploymentOperations;
    private final StrimziPodSetOperator podSetOperations;
    private final PodOperator podOperations;
    private final ConnectBuildOperator connectBuildOperator;
    private final KafkaVersion.Lookup versions;
    private final boolean stableIdentities;
    private final SharedEnvironmentProvider sharedEnvironmentProvider;

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
        this.deploymentOperations = supplier.deploymentOperations;
        this.podSetOperations = supplier.strimziPodSetOperator;
        this.podOperations = supplier.podOperations;
        this.connectBuildOperator = new ConnectBuildOperator(pfa, supplier, config);

        this.versions = config.versions();
        this.stableIdentities = config.featureGates().stableConnectIdentitiesEnabled();
        this.sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;
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

        boolean hasZeroReplicas = connect.getReplicas() == 0;
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
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getComponentName(), stableIdentities ? connect.generateHeadlessService() : null))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, namespace, connect))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(connect.logging().configMapKey());
                    podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    desiredLogging.set(logging);
                    return configMapOperations.reconcile(reconciliation, namespace, logAndMetricsConfigMap.getMetadata().getName(), logAndMetricsConfigMap);
                })
                .compose(i -> ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperations, connect))
                .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generatePodDisruptionBudget(stableIdentities)))
                .compose(i -> generateAuthHash(namespace, kafkaConnect.getSpec()))
                .compose(hash -> {
                    podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    return Future.succeededFuture();
                })
                .compose(i -> {
                    KafkaConnectMigration migration = new KafkaConnectMigration(
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
                    );

                    if (stableIdentities)   {
                        return migration.migrateFromDeploymentToStrimziPodSets(deployment.get(), podSet.get());
                    } else {
                        return migration.migrateFromStrimziPodSetsToDeployment(deployment.get(), podSet.get());
                    }
                })
                .compose(i -> {
                    if (stableIdentities)   {
                        return reconcilePodSet(reconciliation, connect, podAnnotations, controllerAnnotations, image.get());
                    } else {
                        return reconcileDeployment(reconciliation, connect, podAnnotations, controllerAnnotations, image.get(), hasZeroReplicas);
                    }
                })
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
        if (stableIdentities) {
            return podSet != null ? podSet : deployment;
        } else  {
            return deployment != null ? deployment : podSet;
        }
    }

    private Future<Void> reconcileDeployment(Reconciliation reconciliation,
                                             KafkaConnectCluster connect,
                                             Map<String, String> podAnnotations,
                                             Map<String, String> deploymentAnnotations,
                                             String customContainerImage,
                                             boolean hasZeroReplicas)  {
        return deploymentOperations.scaleDown(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.getReplicas(), operationTimeoutMs)
                .compose(i -> deploymentOperations.reconcile(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.generateDeployment(connect.getReplicas(), deploymentAnnotations, podAnnotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, customContainerImage)))
                .compose(i -> deploymentOperations.scaleUp(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.getReplicas(), operationTimeoutMs))
                .compose(i -> deploymentOperations.waitForObserved(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs))
                .compose(i -> hasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs));
    }

    private Future<Void> reconcilePodSet(Reconciliation reconciliation,
                                         KafkaConnectCluster connect,
                                         Map<String, String> podAnnotations,
                                         Map<String, String> podSetAnnotations,
                                         String customContainerImage)  {
        return podSetOperations.reconcile(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.generatePodSet(connect.getReplicas(), podSetAnnotations, podAnnotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, customContainerImage))
                .compose(reconciliationResult -> {
                    KafkaConnectRoller roller = new KafkaConnectRoller(reconciliation, connect, operationTimeoutMs, podOperations);
                    return roller.maybeRoll(reconciliationResult.resource());
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
            Optional<LabelSelector> connectorsSelector = Optional.of(new LabelSelector(List.of(requirement), null));
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

    private boolean isPaused(KafkaConnectorStatus status) {
        return status != null && status.getConditions().stream().anyMatch(condition -> "ReconciliationPaused".equals(condition.getType()));
    }
}
