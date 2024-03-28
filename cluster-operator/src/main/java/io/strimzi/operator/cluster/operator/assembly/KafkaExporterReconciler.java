/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;

import java.time.Clock;
import java.util.List;

/**
 * Class used for reconciliation of Kafka Exporter. This class contains both the steps of the Kafka Exporter
 * reconciliation pipeline and is also used to store the state between them.
 */
public class KafkaExporterReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaExporterReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final KafkaExporter kafkaExporter;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;
    private final boolean isNetworkPolicyGeneration;
    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final NetworkPolicyOperator networkPolicyOperator;

    private boolean existingKafkaExporterCertsChanged = false;

    /**
     * Constructs the Kafka Exporter reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param kafkaAssembly             The Kafka custom resource
     * @param versions                  The supported Kafka versions
     * @param clusterCa                 The Cluster CA instance
     */
    public KafkaExporterReconciler(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            ClusterCa clusterCa
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafkaExporter = KafkaExporter.fromCrd(reconciliation, kafkaAssembly, versions, supplier.sharedEnvironmentProvider);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();

        this.deploymentOperator = supplier.deploymentOperations;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     * @param clock             The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                          That time is used for checking maintenance windows
     *
     * @return                  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, Clock clock)    {
        return serviceAccount()
                .compose(i -> certificatesSecret(clock))
                .compose(i -> networkPolicy())
                .compose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> waitForDeploymentReadiness());
    }

    /**
     * Manages the Kafka Exporter Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaExporterResources.componentName(reconciliation.name()),
                        kafkaExporter != null ? kafkaExporter.generateServiceAccount() : null
                ).map((Void) null);
    }

    /**
     * Manages the Kafka Exporter Secret with certificates.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Future which completes when the reconciliation is done
     */
    private Future<Void> certificatesSecret(Clock clock) {
        if (kafkaExporter != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()),
                                        kafkaExporter.generateSecret(clusterCa, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())))
                                .compose(patchResult -> {
                                    if (patchResult instanceof ReconcileResult.Patched) {
                                        // The secret is patched and some changes to the existing certificates actually occurred
                                        existingKafkaExporterCertsChanged = CertUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
                                    } else {
                                        existingKafkaExporterCertsChanged = false;
                                    }

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the Kafka Exporter Network Policies.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaExporterResources.componentName(reconciliation.name()),
                            kafkaExporter != null ? kafkaExporter.generateNetworkPolicy() : null
                    ).map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Manages the Kafka Exporter deployment.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> deployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (kafkaExporter != null) {
            Deployment deployment = kafkaExporter.generateDeployment(isOpenShift, imagePullPolicy, imagePullSecrets);
            int caCertGeneration = clusterCa.caCertGeneration();
            Annotations.annotations(deployment.getSpec().getTemplate()).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
            int caKeyGeneration = clusterCa.caKeyGeneration();
            Annotations.annotations(deployment.getSpec().getTemplate()).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(caKeyGeneration));

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), deployment)
                    .compose(patchResult -> {
                        if (patchResult instanceof ReconcileResult.Noop)   {
                            // Deployment needs ot be rolled because the certificate secret changed or older/expired cluster CA removed
                            if (existingKafkaExporterCertsChanged || clusterCa.certsRemoved()) {
                                LOGGER.infoCr(reconciliation, "Rolling Kafka Exporter to update or remove certificates");
                                return kafkaExporterRollingUpdate();
                            }
                        }

                        // No need to roll, we patched the deployment (and it will roll itself) or we created a new one
                        return Future.succeededFuture();
                    });
        } else  {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Triggers the rolling update of the Kafka Exporter. This is used to trigger the roll when the certificates change.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> kafkaExporterRollingUpdate() {
        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), operationTimeoutMs);
    }

    /**
     * Waits for the Kafka Exporter deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> waitForDeploymentReadiness() {
        if (kafkaExporter != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
