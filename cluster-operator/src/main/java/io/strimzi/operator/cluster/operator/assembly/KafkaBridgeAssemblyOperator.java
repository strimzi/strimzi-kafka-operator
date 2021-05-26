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
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaBridgeCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Collections;

/**
 * <p>Assembly operator for a "Kafka Bridge" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Bridge Deployment and related Services</li>
 * </ul>
 */
public class KafkaBridgeAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>, KafkaBridgeSpec, KafkaBridgeStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaBridgeAssemblyOperator.class.getName());

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
    protected Future<KafkaBridgeStatus> createOrUpdate(Reconciliation reconciliation, KafkaBridge assemblyResource) {
        KafkaBridgeStatus kafkaBridgeStatus = new KafkaBridgeStatus();

        String namespace = reconciliation.namespace();
        KafkaBridgeCluster bridge;

        try {
            bridge = KafkaBridgeCluster.fromCrd(reconciliation, assemblyResource, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaBridgeStatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(kafkaBridgeStatus, e));
        }

        Promise<KafkaBridgeStatus> createOrUpdatePromise = Promise.promise();

        boolean bridgeHasZeroReplicas = bridge.getReplicas() == 0;
        LOGGER.debugCr(reconciliation, "Updating Kafka Bridge cluster");
        kafkaBridgeServiceAccount(reconciliation, namespace, bridge)
            .compose(i -> deploymentOperations.scaleDown(reconciliation, namespace, bridge.getName(), bridge.getReplicas()))
            .compose(scale -> serviceOperations.reconcile(reconciliation, namespace, bridge.getServiceName(), bridge.generateService()))
            .compose(i -> Util.metricsAndLogging(reconciliation, configMapOperations, namespace, bridge.getLogging(), null))
            .compose(metricsAndLogging -> configMapOperations.reconcile(reconciliation, namespace, bridge.getAncillaryConfigMapName(), bridge.generateMetricsAndLogConfigMap(metricsAndLogging)))
            .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, bridge.getName(), bridge.generatePodDisruptionBudget()))
            .compose(i -> deploymentOperations.reconcile(reconciliation, namespace, bridge.getName(), bridge.generateDeployment(Collections.emptyMap(), pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
            .compose(i -> deploymentOperations.scaleUp(reconciliation, namespace, bridge.getName(), bridge.getReplicas()))
            .compose(i -> deploymentOperations.waitForObserved(reconciliation, namespace, bridge.getName(), 1_000, operationTimeoutMs))
            .compose(i -> bridgeHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(reconciliation, namespace, bridge.getName(), 1_000, operationTimeoutMs))
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

                if (reconciliationResult.succeeded())   {
                    createOrUpdatePromise.complete(kafkaBridgeStatus);
                } else {
                    createOrUpdatePromise.fail(new ReconciliationException(kafkaBridgeStatus, reconciliationResult.cause()));
                }
            });

        return createOrUpdatePromise.future();
    }

    @Override
    protected KafkaBridgeStatus createStatus() {
        return new KafkaBridgeStatus();
    }

    Future<ReconcileResult<ServiceAccount>> kafkaBridgeServiceAccount(Reconciliation reconciliation, String namespace, KafkaBridgeCluster bridge) {
        return serviceAccountOperations.reconcile(reconciliation, namespace,
                KafkaBridgeResources.serviceAccountName(bridge.getCluster()),
                bridge.generateServiceAccount());
    }

    public static Future<ConfigMap> getLoggingCmAsync(ConfigMapOperator configMapOperations, String namespace, KafkaBridgeCluster model) {
        if (model.getLogging() instanceof ExternalLogging) {
            return Util.getExternalLoggingCm(configMapOperations, namespace, (ExternalLogging) model.getLogging());
        } else {
            return Future.succeededFuture(null);
        }
    }
}
