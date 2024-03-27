/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaBridgeCluster;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Collections;
import java.util.List;

/**
 * <p>Assembly operator for a "Kafka Bridge" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Bridge Deployment and related Services</li>
 * </ul>
 */
public class KafkaBridgeAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>, KafkaBridgeSpec, KafkaBridgeStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaBridgeAssemblyOperator.class.getName());

    private final DeploymentOperator deploymentOperations;
    private final SharedEnvironmentProvider sharedEnvironmentProvider;

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
        this.sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;
    }

    @Override
    protected Future<KafkaBridgeStatus> createOrUpdate(Reconciliation reconciliation, KafkaBridge assemblyResource) {
        KafkaBridgeStatus kafkaBridgeStatus = new KafkaBridgeStatus();

        String namespace = reconciliation.namespace();
        KafkaBridgeCluster bridge;

        try {
            bridge = KafkaBridgeCluster.fromCrd(reconciliation, assemblyResource, sharedEnvironmentProvider);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaBridgeStatus, e);
            return Future.failedFuture(new ReconciliationException(kafkaBridgeStatus, e));
        }
        KafkaClientAuthentication auth = assemblyResource.getSpec().getAuthentication();
        List<CertSecretSource> trustedCertificates = assemblyResource.getSpec().getTls() == null ? Collections.emptyList() : assemblyResource.getSpec().getTls().getTrustedCertificates();

        Promise<KafkaBridgeStatus> createOrUpdatePromise = Promise.promise();

        boolean bridgeHasZeroReplicas = bridge.getReplicas() == 0;

        String initCrbName = KafkaBridgeResources.initContainerClusterRoleBindingName(bridge.getCluster(), namespace);
        ClusterRoleBinding initCrb = bridge.generateClusterRoleBinding();

        LOGGER.debugCr(reconciliation, "Updating Kafka Bridge cluster");
        kafkaBridgeServiceAccount(reconciliation, namespace, bridge)
            .compose(i -> bridgeInitClusterRoleBinding(reconciliation, initCrbName, initCrb))
            .compose(i -> deploymentOperations.scaleDown(reconciliation, namespace, bridge.getComponentName(), bridge.getReplicas(), operationTimeoutMs))
            .compose(scale -> serviceOperations.reconcile(reconciliation, namespace, KafkaBridgeResources.serviceName(bridge.getCluster()), bridge.generateService()))
            .compose(i -> MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperations, bridge.logging(), null))
            .compose(metricsAndLogging -> configMapOperations.reconcile(reconciliation, namespace, KafkaBridgeResources.metricsAndLogConfigMapName(reconciliation.name()), bridge.generateMetricsAndLogConfigMap(metricsAndLogging)))
            .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, bridge.getComponentName(), bridge.generatePodDisruptionBudget()))
            .compose(i -> VertxUtil.authTlsHash(secretOperations, namespace, auth, trustedCertificates))
            .compose(hash -> deploymentOperations.reconcile(reconciliation, namespace, bridge.getComponentName(), bridge.generateDeployment(Collections.singletonMap(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash)), pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
            .compose(i -> deploymentOperations.scaleUp(reconciliation, namespace, bridge.getComponentName(), bridge.getReplicas(), operationTimeoutMs))
            .compose(i -> deploymentOperations.waitForObserved(reconciliation, namespace, bridge.getComponentName(), 1_000, operationTimeoutMs))
            .compose(i -> bridgeHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(reconciliation, namespace, bridge.getComponentName(), 1_000, operationTimeoutMs))
            .onComplete(reconciliationResult -> {
                StatusUtils.setStatusConditionAndObservedGeneration(assemblyResource, kafkaBridgeStatus, reconciliationResult.mapEmpty().cause());
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


    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation The Reconciliation identification
     * @return Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return super.delete(reconciliation)
                    .compose(i -> ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaBridgeResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null))
                    .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }


    @Override
    protected KafkaBridgeStatus createStatus(KafkaBridge ignored) {
        return new KafkaBridgeStatus();
    }

    Future<ReconcileResult<ServiceAccount>> kafkaBridgeServiceAccount(Reconciliation reconciliation, String namespace, KafkaBridgeCluster bridge) {
        return serviceAccountOperations.reconcile(reconciliation, namespace,
                KafkaBridgeResources.serviceAccountName(bridge.getCluster()),
                bridge.generateServiceAccount());
    }

    protected Future<ReconcileResult<ClusterRoleBinding>> bridgeInitClusterRoleBinding(Reconciliation reconciliation, String crbName, ClusterRoleBinding crb) {
        return ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, crbName, crb), crb);
    }
}
