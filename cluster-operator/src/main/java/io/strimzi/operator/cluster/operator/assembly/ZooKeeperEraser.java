/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
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
    private final ZookeeperCluster zk;
    private final boolean isNetworkPolicyGeneration;
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
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param kafkaAssembly             The Kafka custom resource
     */
    public ZooKeeperEraser(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Kafka kafkaAssembly
    ) {
        this.reconciliation = reconciliation;
        // NOTE: we are going to use the eraser for deleting ZooKeeper so we are not interested in providing old storage and replicas parameters
        this.zk = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, config.versions(), null, 0, supplier.sharedEnvironmentProvider);
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();

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
                .compose(i -> networkPolicy())
                .compose(i -> serviceAccount())
                .compose(i -> service())
                .compose(i -> headlessService())
                .compose(i -> certificateSecret())
                .compose(i -> loggingAndMetricsConfigMap())
                .compose(i -> podDisruptionBudget())
                .compose(i -> podSet())
                .compose(i -> deletePersistentClaims());
    }

    /**
     * Deletes the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully deleted
     */
    protected Future<Void> jmxSecret() {
        return secretOperator.getAsync(reconciliation.namespace(), zk.jmx().secretName())
                .compose(currentJmxSecret -> {
                    if (currentJmxSecret != null) {
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), zk.jmx().secretName(), null)
                                .map((Void) null);
                    }
                    return Future.succeededFuture();
                });
    }

    /**
     * Deletes the network policy protecting the ZooKeeper cluster
     *
     * @return  Completes when the network policy is successfully deleted
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperNetworkPolicyName(reconciliation.name()), null)
                    .map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Deletes the ZooKeeper service account
     *
     * @return  Completes when the service account was successfully deleted
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the regular CLusterIP service used by ZooKeeper clients
     *
     * @return  Completes when the service was successfully deleted
     */
    protected Future<Void> service() {
        return serviceOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperServiceName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the headless service
     *
     * @return  Completes when the service was successfully deleted
     */
    protected Future<Void> headlessService() {
        return serviceOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the Secret with the node certificates used by the ZooKeeper nodes.
     *
     * @return      Completes when the Secret was successfully deleted
     */
    protected Future<Void> certificateSecret() {
        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperSecretName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the ConfigMap with logging and metrics configuration.
     *
     * @return  Completes when the ConfigMap was successfully deleted
     */
    protected Future<Void> loggingAndMetricsConfigMap() {
        return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperMetricsAndLogConfigMapName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the PodDisruptionBudgets on Kubernetes clusters which support v1 version of PDBs
     *
     * @return  Completes when the PDB was successfully deleted
     */
    protected Future<Void> podDisruptionBudget() {
        return podDisruptionBudgetOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the StrimziPodSet for the ZooKeeper cluster
     *
     * @return  Future which completes when the PodSet is deleted
     */
    protected Future<Void> podSet() {
        return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletion of PVCs is done by honouring the deleteClaim flag.
     * If deleteClaim is set to true (PVCs have Kafka CR as owner), this method deletes the PVCs.
     * If deleteClaim is set to false (PVCs don't have Kafka CR as owner), this method doesn't delete the PVCs (user has to do it manually).
     *
      @return  Future which completes when the PVCs which should be deleted
     */
    protected Future<Void> deletePersistentClaims() {
        return pvcOperator.listAsync(reconciliation.namespace(), zk.getSelectorLabels())
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
