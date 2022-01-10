/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
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
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectDockerfile;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, Resource<KafkaConnect>, KafkaConnectSpec, KafkaConnectStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectAssemblyOperator.class.getName());
    private final DeploymentOperator deploymentOperations;
    private final PodOperator podOperator;
    private final BuildConfigOperator buildConfigOperator;
    private final ConnectBuildOperator connectBuildOperator;
    private final KafkaVersion.Lookup versions;
    protected final long connectBuildTimeoutMs;

    /**
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

    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        this(vertx, pfa, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
    }
    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider, int port) {
        super(vertx, pfa, KafkaConnect.RESOURCE_KIND, supplier.connectOperator, supplier, config, connectClientProvider, port);
        this.deploymentOperations = supplier.deploymentOperations;
        this.podOperator = supplier.podOperations;
        this.buildConfigOperator = supplier.buildConfigOperations;
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

        LOGGER.debugCr(reconciliation, "Updating Kafka Connect cluster");

        boolean connectHasZeroReplicas = connect.getReplicas() == 0;

        final AtomicReference<String> image = new AtomicReference<>();
        final AtomicReference<String> desiredLogging = new AtomicReference<>();
        connectServiceAccount(reconciliation, namespace, KafkaConnectResources.serviceAccountName(connect.getCluster()), connect)
                .compose(i -> connectInitClusterRoleBinding(reconciliation, namespace, kafkaConnect.getMetadata().getName(), connect))
                .compose(i -> connectNetworkPolicy(reconciliation, namespace, connect, isUseResources(kafkaConnect)))
                .compose(i -> connectBuild(reconciliation, namespace, connect.getName(), build))
                .compose(buildState -> {
                    if (buildState.buildRevision != null) {
                        annotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildState.buildRevision);
                    }
                    if (buildState.image != null) {
                        image.set(buildState.image);
                    }
                    return Future.succeededFuture();
                })
                .compose(i -> deploymentOperations.scaleDown(reconciliation, namespace, connect.getName(), connect.getReplicas()))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, namespace, connect))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                    annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH,
                            Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    desiredLogging.set(logging);
                    return configMapOperations.reconcile(reconciliation, namespace, connect.getAncillaryConfigMapName(), logAndMetricsConfigMap);
                })
                .compose(i -> kafkaConnectJmxSecret(reconciliation, namespace, kafkaConnect.getMetadata().getName(), connect))
                .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                .compose(i -> generateAuthHash(namespace, kafkaConnect.getSpec()))
                .compose(hash -> {
                    annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    Deployment deployment = generateDeployment(connect, image.get(), annotations);
                    return deploymentOperations.reconcile(reconciliation, namespace, connect.getName(), deployment);
                })
                .compose(i -> deploymentOperations.scaleUp(reconciliation, namespace, connect.getName(), connect.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(reconciliation, namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> connectHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(reconciliation, namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, kafkaConnect, kafkaConnectStatus, connectHasZeroReplicas, desiredLogging.get(), connect.getDefaultLogConfig()))
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

    /**
     * Creates (or deletes) the ClusterRoleBinding required for the init container used for client rack-awareness.
     * The init-container needs to be able to read the labels from the node it is running on to be able to determine
     * the `client.rack` option.
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace of the service account to which the ClusterRole should be bound
     * @param name              Name of the ClusterRoleBinding
     * @param connectCluster    Name of the Connect cluster
     * @return                  Future for tracking the asynchronous result of the ClusterRoleBinding reconciliation
     */
    Future<ReconcileResult<ClusterRoleBinding>> connectInitClusterRoleBinding(Reconciliation reconciliation, String namespace, String name, KafkaConnectCluster connectCluster) {
        ClusterRoleBinding desired = connectCluster.generateClusterRoleBinding();

        return withIgnoreRbacError(reconciliation,
                clusterRoleBindingOperations.reconcile(reconciliation,
                        KafkaConnectResources.initContainerClusterRoleBindingName(name, namespace),
                        desired),
                desired
        );
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return super.delete(reconciliation)
                .compose(i -> withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaConnectResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null))
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }

    /**
     * Builds a new container image with connectors on Kubernetes using Kaniko or on OpenShift using BuildConfig
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace of the Connect cluster
     * @param connectBuild             KafkaConnectBuild object
     * @return                  Future for tracking the asynchronous result of the Kubernetes image build
     */
    Future<BuildState> connectBuild(Reconciliation reconciliation, String namespace, String connectName, KafkaConnectBuild connectBuild) {
        return deploymentOperations.getAsync(namespace, connectName)
                .compose(deployment -> {
                    String currentBuildRevision = "";
                    String currentImage = "";
                    boolean forceRebuild = false;
                    if (deployment != null) {
                        // Extract information from the current deployment. This is used to figure out if new build needs to be run or not.
                        currentBuildRevision = Annotations.stringAnnotation(deployment.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null);
                        currentImage = deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();
                        forceRebuild = Annotations.hasAnnotation(deployment, Annotations.STRIMZI_IO_CONNECT_FORCE_REBUILD);
                    }
                    BuildState buildState = new BuildState();

                    if (connectBuild.getBuild() != null) {
                        // Build exists => let's build
                        KafkaConnectDockerfile dockerfile = connectBuild.generateDockerfile();
                        String newBuildRevision = dockerfile.hashStub() + Util.sha1Prefix(connectBuild.getBuild().getOutput().getImage());
                        ConfigMap dockerFileConfigMap = connectBuild.generateDockerfileConfigMap(dockerfile);

                        if (newBuildRevision.equals(currentBuildRevision)
                                && !forceRebuild) {
                            // The revision is the same and rebuild was not forced => nothing to do
                            LOGGER.debugCr(reconciliation, "Build configuration did not change. Nothing new to build. Container image {} will be used.", currentImage);
                            buildState.image = currentImage;
                            buildState.buildRevision = newBuildRevision;
                            return Future.succeededFuture(buildState);
                        } else if (pfa.supportsS2I()) {
                            // Revisions differ and we have S2I support => we are on OpenShift and should do a build
                            return connectBuildOperator.openShiftBuild(reconciliation, namespace, connectBuild, forceRebuild, dockerfile, newBuildRevision)
                                    .compose(image -> {
                                        buildState.image = image;
                                        buildState.buildRevision = newBuildRevision;
                                        return Future.succeededFuture(buildState);
                                    });
                        } else {
                            // Revisions differ and no S2I support => we are on Kubernetes and should do a build
                            return connectBuildOperator.kubernetesBuild(reconciliation, namespace, connectBuild, forceRebuild, dockerFileConfigMap, newBuildRevision)
                                    .compose(image -> {
                                        buildState.image = image;
                                        buildState.buildRevision = newBuildRevision;
                                        return Future.succeededFuture(buildState);
                                    });
                        }
                    } else {
                        // Build is not configured => we should delete resources
                        return configMapOperations.reconcile(reconciliation, namespace, KafkaConnectResources.dockerFileConfigMapName(connectBuild.getCluster()), null)
                                .compose(ignore -> podOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), null))
                                .compose(ignore -> serviceAccountOperations.reconcile(reconciliation, namespace, KafkaConnectResources.buildServiceAccountName(connectBuild.getCluster()), null))
                                .compose(ignore -> pfa.supportsS2I() ? buildConfigOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), null) : Future.succeededFuture())
                                .map(i -> buildState);
                    }
                });
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

    /**
     * Utility class to held some helper states for the Kafka Connect Build. This helper class is used to pass the state
     * information around during the reconciliation. But also to make it easier to set the values from inside the lambdas.
     */
    static class BuildState    {
        private String image;
        private String buildRevision;
    }
}
