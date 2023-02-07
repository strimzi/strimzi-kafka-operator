/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

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
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

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
    private final ConnectBuildOperator connectBuildOperator;
    private final KafkaVersion.Lookup versions;
    protected final long connectBuildTimeoutMs;

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
        this.connectBuildOperator = new ConnectBuildOperator(pfa, supplier, config);

        this.versions = config.versions();
        this.connectBuildTimeoutMs = config.getConnectBuildTimeoutMs();
    }

    @Override
    protected Future<KafkaConnectStatus> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        KafkaConnectCluster connect;
        KafkaConnectBuild build;
        KafkaConnectStatus kafkaConnectStatus = new KafkaConnectStatus();
        try {
            connect = KafkaConnectCluster.fromCrd(reconciliation, kafkaConnect, versions);
            build = KafkaConnectBuild.fromCrd(reconciliation, kafkaConnect, versions);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(kafkaConnectStatus, e));
        }

        Promise<KafkaConnectStatus> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();

        Map<String, String> annotations = new HashMap<>(2);

        boolean connectHasZeroReplicas = connect.getReplicas() == 0;
        final AtomicReference<String> image = new AtomicReference<>();
        final AtomicReference<String> desiredLogging = new AtomicReference<>();
        String initCrbName = KafkaConnectResources.initContainerClusterRoleBindingName(kafkaConnect.getMetadata().getName(), namespace);
        ClusterRoleBinding initCrb = connect.generateClusterRoleBinding();

        LOGGER.debugCr(reconciliation, "Updating Kafka Connect cluster");
        connectServiceAccount(reconciliation, namespace, KafkaConnectResources.serviceAccountName(connect.getCluster()), connect)
                .compose(i -> connectInitClusterRoleBinding(reconciliation, initCrbName, initCrb))
                .compose(i -> connectNetworkPolicy(reconciliation, namespace, connect, isUseResources(kafkaConnect)))
                .compose(i -> connectBuildOperator.reconcile(reconciliation, namespace, connect.getComponentName(), build))
                .compose(buildInfo -> {
                    if (buildInfo != null) {
                        annotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildInfo.buildRevision());
                        image.set(buildInfo.image());
                    }
                    return Future.succeededFuture();
                })
                .compose(i -> deploymentOperations.scaleDown(reconciliation, namespace, connect.getComponentName(), connect.getReplicas()))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, namespace, connect))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                    annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH,
                            Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    desiredLogging.set(logging);
                    return configMapOperations.reconcile(reconciliation, namespace, connect.getAncillaryConfigMapName(), logAndMetricsConfigMap);
                })
                .compose(i -> kafkaConnectJmxSecret(reconciliation, namespace, kafkaConnect.getMetadata().getName(), connect))
                .compose(i -> pfa.hasPodDisruptionBudgetV1() ? podDisruptionBudgetOperator.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generatePodDisruptionBudget()) : Future.succeededFuture())
                .compose(i -> !pfa.hasPodDisruptionBudgetV1() ? podDisruptionBudgetV1Beta1Operator.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generatePodDisruptionBudgetV1Beta1()) : Future.succeededFuture())
                .compose(i -> generateAuthHash(namespace, kafkaConnect.getSpec()))
                .compose(hash -> {
                    annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    Deployment deployment = generateDeployment(connect, image.get(), annotations);
                    return deploymentOperations.reconcile(reconciliation, namespace, connect.getComponentName(), deployment);
                })
                .compose(i -> deploymentOperations.scaleUp(reconciliation, namespace, connect.getComponentName(), connect.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(reconciliation, namespace, connect.getComponentName(), 1_000, operationTimeoutMs))
                .compose(i -> connectHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(reconciliation, namespace, connect.getComponentName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, kafkaConnect, kafkaConnectStatus, connectHasZeroReplicas, desiredLogging.get(), connect.defaultLogConfig()))
                .onComplete(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult);

                    if (!connectHasZeroReplicas) {
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

    @Override
    protected KafkaConnectStatus createStatus() {
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
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     *∏@param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return super.delete(reconciliation)
                .compose(i -> ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaConnectResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null))
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
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
        return Util.authTlsHash(secretOperations, namespace, auth, trustedCertificates);
    }

    /**
     * Generates the Connect deployment
     *
     * @param connect           KafkaConnectCluster object
     * @param image             Built image for the Connect deployment
     * @param annotations       Annotations for the deployment
     * @return                  Future for tracking the asynchronous result of getting the metrics and logging config map
     */
    Deployment generateDeployment(KafkaConnectCluster connect, String image, Map<String, String> annotations) {
        Deployment dep = connect.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
        if (image != null) {
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(image);
        }
        return dep;
    }

    private boolean isPaused(KafkaConnectorStatus status) {
        return status != null && status.getConditions().stream().anyMatch(condition -> "ReconciliationPaused".equals(condition.getType()));
    }
}
