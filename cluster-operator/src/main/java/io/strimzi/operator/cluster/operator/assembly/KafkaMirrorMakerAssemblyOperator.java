/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Assembly operator for a "Kafka Mirror Maker" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Mirror Maker Deployment and related Services</li>
 * </ul>
 */
public class KafkaMirrorMakerAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> {

    private static final Logger log = LogManager.getLogger(KafkaMirrorMakerAssemblyOperator.class.getName());

    private final DeploymentOperator deploymentOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaMirrorMakerAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                            CertManager certManager, PasswordGenerator passwordGenerator,
                                            ResourceOperatorSupplier supplier,
                                            ClusterOperatorConfig config) {
        super(vertx, pfa, KafkaMirrorMaker.RESOURCE_KIND, certManager, passwordGenerator, supplier.mirrorMakerOperator, supplier, config);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker assemblyResource) {
        Promise<Void> createOrUpdatePromise = Promise.promise();

        String namespace = reconciliation.namespace();
        KafkaMirrorMakerCluster mirror;
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = new KafkaMirrorMakerStatus();
        if (assemblyResource.getSpec() == null) {
            log.error("{} spec cannot be null", assemblyResource.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        try {
            mirror = KafkaMirrorMakerCluster.fromCrd(assemblyResource, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaMirrorMakerStatus, Future.failedFuture(e));
            updateStatus(assemblyResource, reconciliation, kafkaMirrorMakerStatus);
            return Future.failedFuture(e);
        }

        ConfigMap logAndMetricsConfigMap = mirror.generateMetricsAndLogConfigMap(mirror.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) mirror.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap<>(1);
        annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, logAndMetricsConfigMap.getData().get(mirror.ANCILLARY_CM_KEY_LOG_CONFIG));

        boolean mirrorHasZeroReplicas = mirror.getReplicas() == 0;

        log.debug("{}: Updating Kafka Mirror Maker cluster", reconciliation);
        mirrorMakerServiceAccount(namespace, mirror)
                .compose(i -> deploymentOperations.scaleDown(namespace, mirror.getName(), mirror.getReplicas()))
                .compose(i -> configMapOperations.reconcile(namespace, mirror.getAncillaryConfigMapName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, mirror.getName(), mirror.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, mirror.getName(), mirror.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(namespace, mirror.getName(), mirror.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(namespace, mirror.getName(), 1_000, operationTimeoutMs))
                .compose(i -> mirrorHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(namespace, mirror.getName(), 1_000, operationTimeoutMs))
                .onComplete(reconciliationResult -> {
                        StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaMirrorMakerStatus, reconciliationResult);

                        kafkaMirrorMakerStatus.setReplicas(mirror.getReplicas());
                        kafkaMirrorMakerStatus.setLabelSelector(mirror.getSelectorLabels().toSelectorString());

                        updateStatus(assemblyResource, reconciliation, kafkaMirrorMakerStatus).onComplete(statusResult -> {
                            // If both features succeeded, createOrUpdate succeeded as well
                            // If one or both of them failed, we prefer the reconciliation failure as the main error
                            if (reconciliationResult.succeeded() && statusResult.succeeded()) {
                                createOrUpdatePromise.complete();
                            } else if (reconciliationResult.failed()) {
                                createOrUpdatePromise.fail(reconciliationResult.cause());
                            } else {
                                createOrUpdatePromise.fail(statusResult.cause());
                            }
                        });
                }
            );
        return createOrUpdatePromise.future();
    }

    /**
     * Updates the Status field of the Kafka Mirror Maker CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param kafkaMirrorMakerAssembly The CR of Kafka Mirror Maker
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaMirrorMakerStatus which should be set
     *
     * @return
     */
    Future<Void> updateStatus(KafkaMirrorMaker kafkaMirrorMakerAssembly, Reconciliation reconciliation, KafkaMirrorMakerStatus desiredStatus) {
        Promise<Void> updateStatusPromise = Promise.promise();

        resourceOperator.getAsync(kafkaMirrorMakerAssembly.getMetadata().getNamespace(), kafkaMirrorMakerAssembly.getMetadata().getName()).onComplete(getRes -> {
            if (getRes.succeeded()) {
                KafkaMirrorMaker mirrorMaker = getRes.result();

                if (mirrorMaker != null) {
                    if (StatusUtils.isResourceV1alpha1(mirrorMaker)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, mirrorMaker.getApiVersion());
                        updateStatusPromise.complete();
                    } else {
                        KafkaMirrorMakerStatus currentStatus = mirrorMaker.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            KafkaMirrorMaker resourceWithNewStatus = new KafkaMirrorMakerBuilder(mirrorMaker).withStatus(desiredStatus).build();

                            ((CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker>) resourceOperator).updateStatusAsync(resourceWithNewStatus).onComplete(updateRes -> {
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
                    log.error("{}: Current Kafka Mirror Maker resource not found", reconciliation);
                    updateStatusPromise.fail("Current Kafka Mirror Maker resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka Mirror Maker resource and its status", reconciliation, getRes.cause());
                updateStatusPromise.fail(getRes.cause());
            }
        });
        return updateStatusPromise.future();
    }

    Future<ReconcileResult<ServiceAccount>> mirrorMakerServiceAccount(String namespace, KafkaMirrorMakerCluster mirror) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaMirrorMakerResources.serviceAccountName(mirror.getCluster()),
                mirror.generateServiceAccount());
    }
}
