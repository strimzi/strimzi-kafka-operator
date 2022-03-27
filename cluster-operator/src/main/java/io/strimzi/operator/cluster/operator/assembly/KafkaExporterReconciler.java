/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;

import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

/**
 * Class used for reconciliation of Kafka Exporter. This class contains both the steps of the Kafka Exporter
 * reconciliation pipeline and is also used to store the state between them.
 */
public class KafkaExporterReconciler {
    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final KafkaExporter kafkaExporter;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;

    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;

    private boolean existingKafkaExporterCertsChanged = false;

    /**
     * Constructs the Kafka Exporter reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param operationTimeoutMs        Timeout for Kubernetes operations
     * @param kafkaAssembly             The Kafka custom resource
     * @param versions                  The supported Kafka versions
     * @param clusterCa                 The Cluster CA instance
     * @param deploymentOperator        The Deployment operator for working with Kubernetes Deployments
     * @param secretOperator            The Secret operator for working with Kubernetes Secrets
     * @param serviceAccountOperator    The Service Account operator for working with Kubernetes Service Accounts
     */
    public KafkaExporterReconciler(
            Reconciliation reconciliation,
            long operationTimeoutMs,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            ClusterCa clusterCa,
            DeploymentOperator deploymentOperator,
            SecretOperator secretOperator,
            ServiceAccountOperator serviceAccountOperator
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = operationTimeoutMs;
        this.kafkaExporter = KafkaExporter.fromCrd(reconciliation, kafkaAssembly, versions);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();

        this.deploymentOperator = deploymentOperator;
        this.secretOperator = secretOperator;
        this.serviceAccountOperator = serviceAccountOperator;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     * @param dateSupplier      Date supplier for checking maintenance windows
     *
     * @return                  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, Supplier<Date> dateSupplier)    {
        return kafkaExporterServiceAccount()
                .compose(i -> kafkaExporterSecret(dateSupplier))
                .compose(i -> kafkaExporterDeployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> kafkaExporterReady());
    }

    /**
     * Manages the Kafka Exporter Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> kafkaExporterServiceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaExporterResources.deploymentName(reconciliation.name()),
                        kafkaExporter != null ? kafkaExporter.generateServiceAccount() : null
                ).map((Void) null);
    }

    /**
     * Manages the Kafka Exporter Secret with certificates.
     *
     * @param dateSupplier  Date supplier for checking maintenance windows for updating the certificates
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> kafkaExporterSecret(Supplier<Date> dateSupplier) {
        if (kafkaExporter != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()),
                                        kafkaExporter.generateSecret(clusterCa, ModelUtils.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier)))
                                .compose(patchResult -> {
                                    if (patchResult instanceof ReconcileResult.Patched) {
                                        // The secret is patched and some changes to the existing certificates actually occurred
                                        existingKafkaExporterCertsChanged = ModelUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
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
     * Manages the Kafka Exporter deployment.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> kafkaExporterDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (kafkaExporter != null) {
            Deployment deployment = kafkaExporter.generateDeployment(isOpenShift, imagePullPolicy, imagePullSecrets);
            int caCertGeneration = ModelUtils.caCertGeneration(this.clusterCa);
            Annotations.annotations(deployment.getSpec().getTemplate()).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.deploymentName(reconciliation.name()), deployment)
                    .compose(patchResult -> {
                        if (patchResult instanceof ReconcileResult.Noop)   {
                            // Deployment needs ot be rolled because the certificate secret changed
                            if (existingKafkaExporterCertsChanged) {
                                return kafkaExporterRollingUpdate();
                            }
                        }

                        // No need to roll, we patched the deployment (and it will roll itself) or we created a new one
                        return Future.succeededFuture();
                    });
        } else  {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.deploymentName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Triggers the rolling update of the Kafka Exporter. This is used to trigger the roll when the certificates change.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> kafkaExporterRollingUpdate() {
        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), KafkaExporterResources.deploymentName(reconciliation.name()), operationTimeoutMs);
    }

    /**
     * Waits for the Kafka Exporter deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> kafkaExporterReady() {
        if (kafkaExporter != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), KafkaExporterResources.deploymentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), KafkaExporterResources.deploymentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
