/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaConnectS2ISpec;
import io.strimzi.api.kafka.model.status.KafkaConnectS2IStatus;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * <p>Assembly operator for a "Kafka Connect S2I" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>An ImageBuildStream</li>
 *     <li>A BuildConfig</li>
 * </ul>
 */
public class KafkaConnectS2IAssemblyOperator extends AbstractConnectOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>, KafkaConnectS2ISpec, KafkaConnectS2IStatus> {
    private static final Logger log = LogManager.getLogger(KafkaConnectS2IAssemblyOperator.class.getName());
    
    private final DeploymentConfigOperator deploymentConfigOperations;
    private final ImageStreamOperator imagesStreamOperations;
    private final BuildConfigOperator buildConfigOperations;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final KafkaVersion.Lookup versions;
    private final CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect> connectOperations;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public KafkaConnectS2IAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,

                                           ResourceOperatorSupplier supplier,
                                           ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    public KafkaConnectS2IAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                           ResourceOperatorSupplier supplier,
                                           ClusterOperatorConfig config,
                                           Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, KafkaConnectS2I.RESOURCE_KIND, supplier.connectS2IOperator, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
        this.deploymentConfigOperations = supplier.deploymentConfigOperations;
        this.imagesStreamOperations = supplier.imagesStreamOperations;
        this.buildConfigOperations = supplier.buildConfigOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.connectOperations = supplier.connectOperator;
        this.versions = config.versions();

    }

    @Override
    public Future<KafkaConnectS2IStatus> createOrUpdate(Reconciliation reconciliation, KafkaConnectS2I kafkaConnectS2I) {
        KafkaConnectS2ICluster connect;
        KafkaConnectS2IStatus kafkaConnectS2Istatus = new KafkaConnectS2IStatus();

        try {
            connect = KafkaConnectS2ICluster.fromCrd(kafkaConnectS2I, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnectS2I, kafkaConnectS2Istatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(kafkaConnectS2Istatus, e));
        }

        Promise<KafkaConnectS2IStatus> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();

        connect.generateBuildConfig();

        Map<String, String> annotations = new HashMap<>(1);
        final AtomicReference<String> desiredLogging = new AtomicReference<>();

        boolean connectHasZeroReplicas = connect.getReplicas() == 0;

        log.debug("{}: Updating Kafka Connect S2I cluster", reconciliation);

        connectOperations.getAsync(kafkaConnectS2I.getMetadata().getNamespace(), kafkaConnectS2I.getMetadata().getName())
                .compose(otherConnect -> {
                    if (otherConnect != null
                            // There is a KafkaConnect with the same name which is older than  or equally old as this KafkaConnectS2I
                            && kafkaConnectS2I.getMetadata().getCreationTimestamp().compareTo(otherConnect.getMetadata().getCreationTimestamp()) >= 0)    {
                        return Future.failedFuture("Both KafkaConnect and KafkaConnectS2I exist with the same name. " +
                                "KafkaConnect is older and will be used while this custom resource will be ignored.");
                    } else {
                        return Future.succeededFuture();
                    }
                })
                .compose(i -> {
                    if (pfa.hasImages() && pfa.hasApps() && pfa.hasBuilds()) {
                        return Future.succeededFuture();
                    } else {
                        return Future.failedFuture("The OpenShift build, image or apps APIs are not available in this Kubernetes cluster. " +
                                "Kafka Connect S2I deployment cannot be enabled.");
                    }
                })
                .compose(i -> connectServiceAccount(namespace, connect))
                .compose(i -> networkPolicyOperator.reconcile(namespace, connect.getName(), connect.generateNetworkPolicy(pfa.isNamespaceAndPodSelectorNetworkPolicySupported(), isUseResources(kafkaConnectS2I), operatorNamespace, operatorNamespaceLabels)))
                .compose(i -> deploymentConfigOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> connectMetricsAndLoggingConfigMap(namespace, connect))
                .compose(metricsAndLoggingCm -> {
                    ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(metricsAndLoggingCm.loggingCm, metricsAndLoggingCm.metricsCm);
                    annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH,
                            Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG))));
                    desiredLogging.set(logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG));
                    return configMapOperations.reconcile(namespace, connect.getAncillaryConfigMapName(), logAndMetricsConfigMap);
                })
                .compose(i -> deploymentConfigOperations.reconcile(namespace, connect.getName(), connect.generateDeploymentConfig(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> imagesStreamOperations.reconcile(namespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()), connect.generateSourceImageStream()))
                .compose(i -> imagesStreamOperations.reconcile(namespace, KafkaConnectS2IResources.targetImageStreamName(connect.getCluster()), connect.generateTargetImageStream()))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                .compose(i -> buildConfigOperations.reconcile(namespace, KafkaConnectS2IResources.buildConfigName(connect.getCluster()), connect.generateBuildConfig()))
                .compose(i -> deploymentConfigOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()))
                .compose(i -> deploymentConfigOperations.waitForObserved(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> connectHasZeroReplicas ? Future.succeededFuture() : deploymentConfigOperations.readiness(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, kafkaConnectS2I, kafkaConnectS2Istatus, connectHasZeroReplicas, desiredLogging.get(), connect.getDefaultLogConfig()))
                .onComplete(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnectS2I, kafkaConnectS2Istatus, reconciliationResult);

                    if (!connectHasZeroReplicas) {
                        kafkaConnectS2Istatus.setUrl(KafkaConnectS2IResources.url(connect.getCluster(), namespace, KafkaConnectS2ICluster.REST_API_PORT));
                        kafkaConnectS2Istatus.setBuildConfigName(KafkaConnectS2IResources.buildConfigName(connect.getCluster()));
                    }

                    kafkaConnectS2Istatus.setReplicas(connect.getReplicas());
                    kafkaConnectS2Istatus.setLabelSelector(connect.getSelectorLabels().toSelectorString());

                    if (reconciliationResult.succeeded())   {
                        createOrUpdatePromise.complete(kafkaConnectS2Istatus);
                    } else {
                        createOrUpdatePromise.fail(new ReconciliationException(kafkaConnectS2Istatus, reconciliationResult.cause()));
                    }
                });
        return createOrUpdatePromise.future();
    }

    @Override
    protected KafkaConnectS2IStatus createStatus() {
        return new KafkaConnectS2IStatus();
    }

    /**
     * Updates the Status field of the Kafka ConnectS2I CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param kafkaConnectS2Iassembly The CR of Kafka ConnectS2I
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaConnectS2Istatus which should be set
     *
     * @return
     */
    Future<Void> updateStatus(KafkaConnectS2I kafkaConnectS2Iassembly, Reconciliation reconciliation, KafkaConnectS2IStatus desiredStatus) {
        Promise<Void> updateStatusPromise = Promise.promise();

        resourceOperator.getAsync(kafkaConnectS2Iassembly.getMetadata().getNamespace(), kafkaConnectS2Iassembly.getMetadata().getName()).onComplete(getRes -> {
            if (getRes.succeeded()) {
                KafkaConnectS2I connect = getRes.result();

                if (connect != null) {
                    if (StatusUtils.isResourceV1alpha1(connect)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, connect.getApiVersion());
                        updateStatusPromise.complete();
                    } else {
                        KafkaConnectS2IStatus currentStatus = connect.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            KafkaConnectS2I resourceWithNewStatus = new KafkaConnectS2IBuilder(connect).withStatus(desiredStatus).build();

                            ((CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I>) resourceOperator).updateStatusAsync(resourceWithNewStatus).onComplete(updateRes -> {
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
                    log.error("{}: Current Kafka ConnectS2I resource not found", reconciliation);
                    updateStatusPromise.fail("Current Kafka ConnectS2I resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka ConnectS2I resource and its status", reconciliation, getRes.cause());
                updateStatusPromise.fail(getRes.cause());
            }
        });

        return updateStatusPromise.future();
    }

    Future<ReconcileResult<ServiceAccount>> connectServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectS2IResources.serviceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
