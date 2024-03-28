/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class used for deleting the ZooKeeper ensemble when the ZooKeeper to KRaft migration is completed
 * and the Kafka cluster is full KRaft, not using ZooKeeper anymore for storing metadata
 */
public class ZooKeeperEraser {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZooKeeperEraser.class.getName());

    private final Reconciliation reconciliation;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ServiceOperator serviceOperator;
    private final PvcOperator pvcOperator;
    private final StorageClassOperator storageClassOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;

    /**
     * Constructs the ZooKeeper eraser
     *
     * @param reconciliation            Reconciliation marker
     * @param supplier                  Supplier with Kubernetes Resource Operators
     */
    public ZooKeeperEraser(
            Reconciliation reconciliation,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;

        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.serviceOperator = supplier.serviceOperations;
        this.pvcOperator = supplier.pvcOperations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @return              Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile()    {
        LOGGER.infoCr(reconciliation, "Deleting all the ZooKeeper related resources");
        return jmxSecret()
                .compose(i -> deleteNetworkPolicy())
                .compose(i -> deleteServiceAccount())
                .compose(i -> deleteService())
                .compose(i -> deleteHeadlessService())
                .compose(i -> deleteCertificateSecret())
                .compose(i -> deleteLoggingAndMetricsConfigMap())
                .compose(i -> deletePodDisruptionBudget())
                .compose(i -> deletePodSet())
                .compose(i -> deletePersistentClaims());
    }

    /**
     * Deletes the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully deleted
     */
    protected Future<Void> jmxSecret() {
        return secretOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperJmxSecretName(reconciliation.name()), true);
    }

    /**
     * Deletes the network policy protecting the ZooKeeper cluster
     *
     * @return  Completes when the network policy is successfully deleted
     */
    protected Future<Void> deleteNetworkPolicy() {
        return networkPolicyOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperNetworkPolicyName(reconciliation.name()), true);
    }

    /**
     * Deletes the ZooKeeper service account
     *
     * @return  Completes when the service account was successfully deleted
     */
    protected Future<Void> deleteServiceAccount() {
        return serviceAccountOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), true);
    }

    /**
     * Deletes the regular CLusterIP service used by ZooKeeper clients
     *
     * @return  Completes when the service was successfully deleted
     */
    protected Future<Void> deleteService() {
        return serviceOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperServiceName(reconciliation.name()), true);
    }

    /**
     * Deletes the headless service
     *
     * @return  Completes when the service was successfully deleted
     */
    protected Future<Void> deleteHeadlessService() {
        return serviceOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), true);
    }

    /**
     * Deletes the Secret with the node certificates used by the ZooKeeper nodes.
     *
     * @return      Completes when the Secret was successfully deleted
     */
    protected Future<Void> deleteCertificateSecret() {
        return secretOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperSecretName(reconciliation.name()), true);
    }

    /**
     * Deletes the ConfigMap with logging and metrics configuration.
     *
     * @return  Completes when the ConfigMap was successfully deleted
     */
    protected Future<Void> deleteLoggingAndMetricsConfigMap() {
        return configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperMetricsAndLogConfigMapName(reconciliation.name()), true);
    }

    /**
     * Deletes the PodDisruptionBudgets on Kubernetes clusters which support v1 version of PDBs
     *
     * @return  Completes when the PDB was successfully deleted
     */
    protected Future<Void> deletePodDisruptionBudget() {
        return podDisruptionBudgetOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), true);
    }

    /**
     * Deletes the StrimziPodSet for the ZooKeeper cluster
     *
     * @return  Future which completes when the PodSet is deleted
     */
    protected Future<Void> deletePodSet() {
        return strimziPodSetOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), true);
    }

    /**
     * Deletion of PVCs is done by honouring the deleteClaim flag.
     * If deleteClaim is set to true (PVCs have Kafka CR as owner), this method deletes the PVCs.
     * If deleteClaim is set to false (PVCs don't have Kafka CR as owner), this method doesn't delete the PVCs (user has to do it manually).
     *
      @return  Future which completes when the PVCs which should be deleted
     */
    protected Future<Void> deletePersistentClaims() {
        Labels zkSelectorLabels = Labels.EMPTY
                .withStrimziKind(reconciliation.kind())
                .withStrimziCluster(reconciliation.name())
                .withStrimziName(KafkaResources.zookeeperComponentName(reconciliation.name()));
        return pvcOperator.listAsync(reconciliation.namespace(), zkSelectorLabels)
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    // not having owner reference on the PVC means corresponding spec.zookeeper.storage.deleteClaim is false, so we don't want to delete
                    // the corresponding PVC when ZooKeeper is removed
                    List<String> desiredPvcs = pvcs.stream().filter(pvc -> pvc.getMetadata().getOwnerReferences() == null).map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                });
    }
}
