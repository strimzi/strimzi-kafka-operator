/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaBridgeCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
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

import java.util.Collections;

/**
 * <p>Assembly operator for a "Kafka Bridge" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Bridge Deployment and related Services</li>
 * </ul>
 */
public class KafkaBridgeAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge, Resource<KafkaBridge, DoneableKafkaBridge>> {
    private static final Logger log = LogManager.getLogger(KafkaBridgeAssemblyOperator.class.getName());

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
    public KafkaBridgeAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                       CertManager certManager, PasswordGenerator passwordGenerator,
                                       ResourceOperatorSupplier supplier,
                                       ClusterOperatorConfig config) {
        super(vertx, pfa, KafkaBridge.RESOURCE_KIND, certManager, passwordGenerator, supplier.kafkaBridgeOperator, supplier, config);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaBridge assemblyResource) {
        Promise<Void> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();
        KafkaBridgeCluster bridge;
        KafkaBridgeStatus kafkaBridgeStatus = new KafkaBridgeStatus();
        if (assemblyResource.getSpec() == null) {
            log.error("{} spec cannot be null", assemblyResource.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        try {
            bridge = KafkaBridgeCluster.fromCrd(assemblyResource, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaBridgeStatus, Future.failedFuture(e));
            return updateStatus(assemblyResource, reconciliation, kafkaBridgeStatus);
        }

        ConfigMap logAndMetricsConfigMap = bridge.generateMetricsAndLogConfigMap(bridge.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) bridge.getLogging()).getName()) :
                null);

        boolean bridgeHasZeroReplicas = bridge.getReplicas() == 0;
        log.debug("{}: Updating Kafka Bridge cluster", reconciliation);
        kafkaBridgeServiceAccount(namespace, bridge)
            .compose(i -> deploymentOperations.scaleDown(namespace, bridge.getName(), bridge.getReplicas()))
            .compose(scale -> serviceOperations.reconcile(namespace, bridge.getServiceName(), bridge.generateService()))
            .compose(i -> configMapOperations.reconcile(namespace, bridge.getAncillaryConfigMapName(), logAndMetricsConfigMap))
            .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, bridge.getName(), bridge.generatePodDisruptionBudget()))
            .compose(i -> deploymentOperations.reconcile(namespace, bridge.getName(), bridge.generateDeployment(Collections.emptyMap(), pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
            .compose(i -> deploymentOperations.scaleUp(namespace, bridge.getName(), bridge.getReplicas()))
            .compose(i -> deploymentOperations.waitForObserved(namespace, bridge.getName(), 1_000, operationTimeoutMs))
            .compose(i -> bridgeHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(namespace, bridge.getName(), 1_000, operationTimeoutMs))
            .onComplete(reconciliationResult -> {
                StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaBridgeStatus, reconciliationResult.mapEmpty());
                if (!bridgeHasZeroReplicas) {
                    int port = KafkaBridgeCluster.DEFAULT_REST_API_PORT;
                    if (bridge.getHttp() != null) {
                        port = bridge.getHttp().getPort();
                    }
                    kafkaBridgeStatus.setUrl(KafkaBridgeResources.url(bridge.getCluster(), namespace, port));
                }

                kafkaBridgeStatus.setReplicas(bridge.getReplicas());
                kafkaBridgeStatus.setLabelSelector(bridge.getSelectorLabels().toSelectorString());

                updateStatus(assemblyResource, reconciliation, kafkaBridgeStatus).onComplete(statusResult -> {
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
            });

        return createOrUpdatePromise.future();
    }

    /**
     * Updates the Status field of the Kafka Bridge CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param kafkaBridgeAssembly The CR of Kafka Bridge
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaBridgeStatus which should be set
     *
     * @return
     */
    Future<Void> updateStatus(KafkaBridge kafkaBridgeAssembly, Reconciliation reconciliation, KafkaBridgeStatus desiredStatus) {
        Promise<Void> updateStatusPromise = Promise.promise();

        resourceOperator.getAsync(kafkaBridgeAssembly.getMetadata().getNamespace(), kafkaBridgeAssembly.getMetadata().getName()).onComplete(getRes -> {
            if (getRes.succeeded()) {
                KafkaBridge kafkaBridge = getRes.result();

                if (kafkaBridge != null) {
                    KafkaBridgeStatus currentStatus = kafkaBridge.getStatus();

                    StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                    if (!ksDiff.isEmpty()) {
                        KafkaBridge resourceWithNewStatus = new KafkaBridgeBuilder(kafkaBridge).withStatus(desiredStatus).build();
                        ((CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge>) resourceOperator).updateStatusAsync(resourceWithNewStatus).onComplete(updateRes -> {
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
                } else {
                    log.error("{}: Current Kafka Bridge resource not found", reconciliation);
                    updateStatusPromise.fail("Current Kafka Bridge resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka Bridge resource and its status", reconciliation, getRes.cause());
                updateStatusPromise.fail(getRes.cause());
            }
        });

        return updateStatusPromise.future();
    }

    Future<ReconcileResult<ServiceAccount>> kafkaBridgeServiceAccount(String namespace, KafkaBridgeCluster bridge) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaBridgeResources.serviceAccountName(bridge.getCluster()),
                bridge.generateServiceAccount());
    }
}
