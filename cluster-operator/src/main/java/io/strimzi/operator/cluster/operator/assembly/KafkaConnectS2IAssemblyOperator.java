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
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.status.KafkaConnectS2Istatus;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.function.Function;

/**
 * <p>Assembly operator for a "Kafka Connect S2I" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>An ImageBuildStream</li>
 *     <li>A BuildConfig</li>
 * </ul>
 */
public class KafkaConnectS2IAssemblyOperator extends AbstractConnectOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> {

    private static final Logger log = LogManager.getLogger(KafkaConnectS2IAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";
    private final DeploymentConfigOperator deploymentConfigOperations;
    private final ImageStreamOperator imagesStreamOperations;
    private final BuildConfigOperator buildConfigOperations;
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
        super(vertx, pfa, KafkaConnectS2I.RESOURCE_KIND, supplier.connectS2IOperator, supplier, config, connectClientProvider);
        this.deploymentConfigOperations = supplier.deploymentConfigOperations;
        this.imagesStreamOperations = supplier.imagesStreamOperations;
        this.buildConfigOperations = supplier.buildConfigOperations;
        this.connectOperations = supplier.connectOperator;
        this.versions = config.versions();

    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnectS2I kafkaConnectS2I) {
        String namespace = reconciliation.namespace();
        if (kafkaConnectS2I.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaConnectS2I.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        if (pfa.hasImages() && pfa.hasApps() && pfa.hasBuilds()) {
            Future<Void> createOrUpdateFuture = Future.future();
            KafkaConnectS2ICluster connect;
            KafkaConnectS2Istatus kafkaConnectS2Istatus = new KafkaConnectS2Istatus();
            try {
                connect = KafkaConnectS2ICluster.fromCrd(kafkaConnectS2I, versions);
            } catch (Exception e) {
                StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnectS2I, kafkaConnectS2Istatus, Future.failedFuture(e));
                return updateStatus(kafkaConnectS2I, reconciliation, kafkaConnectS2Istatus);
            }
            connect.generateBuildConfig();
            ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                    configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                    null);

            HashMap<String, String> annotations = new HashMap<>();
            annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(connect.ANCILLARY_CM_KEY_LOG_CONFIG));
            Future<Void> chainFuture = Future.future();
            connectServiceAccount(namespace, connect)
                    .compose(i -> deploymentConfigOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                    .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                    .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigName(), logAndMetricsConfigMap))
                    .compose(i -> deploymentConfigOperations.reconcile(namespace, connect.getName(), connect.generateDeploymentConfig(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                    .compose(i -> imagesStreamOperations.reconcile(namespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()), connect.generateSourceImageStream()))
                    .compose(i -> imagesStreamOperations.reconcile(namespace, KafkaConnectS2IResources.targetImageStreamName(connect.getCluster()), connect.generateTargetImageStream()))
                    .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                    .compose(i -> buildConfigOperations.reconcile(namespace, KafkaConnectS2IResources.buildConfigName(connect.getCluster()), connect.generateBuildConfig()))
                    .compose(i -> deploymentConfigOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()))
                    .compose(i -> deploymentConfigOperations.waitForObserved(namespace, connect.getName(), 1_000, operationTimeoutMs))
                    .compose(i -> deploymentConfigOperations.readiness(namespace, connect.getName(), 1_000, operationTimeoutMs))
                    .compose(i -> reconcileConnectors(reconciliation, kafkaConnectS2I))
                    .compose(i -> chainFuture.complete(), chainFuture)
                    .setHandler(reconciliationResult -> {
                        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnectS2I, kafkaConnectS2Istatus, reconciliationResult);
                        kafkaConnectS2Istatus.setUrl(KafkaConnectS2IResources.url(connect.getCluster(), namespace, KafkaConnectS2ICluster.REST_API_PORT));
                        kafkaConnectS2Istatus.setBuildConfigName(KafkaConnectS2IResources.buildConfigName(connect.getCluster()));

                        updateStatus(kafkaConnectS2I, reconciliation, kafkaConnectS2Istatus).setHandler(statusResult -> {
                            // If both features succeeded, createOrUpdate succeeded as well
                            // If one or both of them failed, we prefer the reconciliation failure as the main error
                            if (reconciliationResult.succeeded() && statusResult.succeeded()) {
                                createOrUpdateFuture.complete();
                            } else if (reconciliationResult.failed()) {
                                createOrUpdateFuture.fail(reconciliationResult.cause());
                            } else {
                                createOrUpdateFuture.fail(statusResult.cause());
                            }
                        });
                    });
            return createOrUpdateFuture;

        } else {
            return Future.failedFuture("The OpenShift build, image or apps APIs are not available in this Kubernetes cluster. Kafka Connect S2I deployment cannot be enabled.");
        }
    }

    @Override
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, KafkaConnectS2I connects2i) {
        return connectOperations.getAsync(connects2i.getMetadata().getNamespace(), connects2i.getMetadata().getName()).compose(connect -> {
            // If there's a non-s2i of the same name then do nothing, since that takes precedence
            if (connect == null) {
                return Future.succeededFuture();
            } else {
                return super.reconcileConnectors(reconciliation, connects2i);
            }
        });
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
    Future<Void> updateStatus(KafkaConnectS2I kafkaConnectS2Iassembly, Reconciliation reconciliation, KafkaConnectS2Istatus desiredStatus) {
        Future<Void> updateStatusFuture = Future.future();

        resourceOperator.getAsync(kafkaConnectS2Iassembly.getMetadata().getNamespace(), kafkaConnectS2Iassembly.getMetadata().getName()).setHandler(getRes -> {
            if (getRes.succeeded()) {
                KafkaConnectS2I connect = getRes.result();

                if (connect != null) {
                    if (StatusUtils.isResourceV1alpha1(connect)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, connect.getApiVersion());
                        updateStatusFuture.complete();
                    } else {
                        KafkaConnectS2Istatus currentStatus = connect.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            KafkaConnectS2I resourceWithNewStatus = new KafkaConnectS2IBuilder(connect).withStatus(desiredStatus).build();

                            ((CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I>) resourceOperator).updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
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
                    log.error("{}: Current Kafka ConnectS2I resource not found", reconciliation);
                    updateStatusFuture.fail("Current Kafka ConnectS2I resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka ConnectS2I resource and its status", reconciliation, getRes.cause());
                updateStatusFuture.fail(getRes.cause());
            }
        });

        return updateStatusFuture;
    }

    Future<ReconcileResult<ServiceAccount>> connectServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectResources.serviceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
