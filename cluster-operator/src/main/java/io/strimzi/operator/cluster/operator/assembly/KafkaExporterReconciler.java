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
import io.strimzi.operator.cluster.model.CertSecretUtils;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Class used for reconciliation of Kafka Exporter. This class contains both the steps of the Kafka Exporter
 * reconciliation pipeline and is also used to store the state between them.
 */
public class KafkaExporterReconciler {
    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final KafkaExporter kafkaExporter;
    private final Ca clusterCa;
    private final List<String> maintenanceWindows;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isPodDisruptionBudgetGeneration;
    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;

    private String certificateHash = "";

    /**
     * Constructs the Kafka Exporter reconciler
     *
     * @param reconciliation    Reconciliation marker
     * @param config            Cluster Operator Configuration
     * @param supplier          Supplier with Kubernetes Resource Operators
     * @param kafkaAssembly     The Kafka custom resource
     * @param versions          The supported Kafka versions
     * @param clusterCa         The Cluster CA instance
     * @param securityContext   Kafka cluster security context
     */
    public KafkaExporterReconciler(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            Ca clusterCa,
            KafkaClusterSecurityContext securityContext) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafkaExporter = KafkaExporter.fromCrd(reconciliation, kafkaAssembly, versions, supplier.sharedEnvironmentProvider, securityContext);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.isPodDisruptionBudgetGeneration = config.isPodDisruptionBudgetGeneration();

        this.deploymentOperator = supplier.deploymentOperations;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
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
     * @return                  CompletionStage which completes when the reconciliation completes
     */
    public CompletionStage<Void> reconcile(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, Clock clock)    {
        return serviceAccount()
                .thenCompose(i -> certificatesSecret(clock))
                .thenCompose(i -> networkPolicy())
                .thenCompose(i -> podDisruptionBudget())
                .thenCompose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .thenCompose(i -> waitForDeploymentReadiness());
    }

    /**
     * Manages the Kafka Exporter Service Account.
     *
     * @return  CompletionStage which completes when the reconciliation is done
     */
    private CompletionStage<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaExporterResources.componentName(reconciliation.name()),
                        kafkaExporter != null ? kafkaExporter.generateServiceAccount() : null
                ).thenApply(i -> null);
    }

    /**
     * Manages the Kafka Exporter Secret with certificates.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      CompletionStage which completes when the reconciliation is done
     */
    private CompletionStage<Void> certificatesSecret(Clock clock) {
        if (kafkaExporter != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()))
                    .thenCompose(oldSecret -> kafkaExporter.generateCertificatesSecret(clusterCa, oldSecret, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())))
                    .thenCompose(newSecret -> secretOperator
                            .reconcile(reconciliation,
                                    reconciliation.namespace(),
                                    KafkaExporterResources.secretName(reconciliation.name()),
                                    newSecret)
                            .thenCompose(result -> {
                                certificateHash = CertSecretUtils.getCertificateShortThumbprint(newSecret, Ca.SecretEntry.CRT.asKey(KafkaExporter.COMPONENT_TYPE));

                                return CompletableFuture.completedFuture(null);
                            })
                    );
        } else {
            return secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.secretName(reconciliation.name()), null)
                    .thenApply(i -> null);
        }
    }

    /**
     * Manages the Kafka Exporter Network Policies.
     *
     * @return  CompletionStage which completes when the reconciliation is done
     */
    protected CompletionStage<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaExporterResources.componentName(reconciliation.name()),
                            kafkaExporter != null ? kafkaExporter.generateNetworkPolicy() : null
                    ).thenApply(i -> null);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Manages the Kafka Exporter Pod Disruption Budget
     *
     * @return  CompletionStage which completes when the reconciliation is done
     */
    protected CompletionStage<Void> podDisruptionBudget() {
        if (isPodDisruptionBudgetGeneration) {
            return podDisruptionBudgetOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaExporterResources.componentName(reconciliation.name()),
                            kafkaExporter != null ? kafkaExporter.generatePodDisruptionBudget() : null
                    ).thenApply(i -> null);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
    /**
     * Manages the Kafka Exporter deployment.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     *
     * @return  CompletionState which completes when the reconciliation is done
     */
    private CompletionStage<Void> deployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (kafkaExporter != null) {
            Map<String, String> podAnnotations = new LinkedHashMap<>();
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCa.caCertGeneration()));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()));
            podAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, certificateHash);

            Deployment deployment = kafkaExporter.generateDeployment(podAnnotations, isOpenShift, imagePullPolicy, imagePullSecrets);

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), deployment)
                    .thenApply(i -> null);
        } else  {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), null)
                    .thenApply(i -> null);
        }
    }

    /**
     * Waits for the Kafka Exporter deployment to finish any rolling and get ready.
     *
     * @return  CompletionStage which completes when the reconciliation is done
     */
    private CompletionStage<Void> waitForDeploymentReadiness() {
        if (kafkaExporter != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .thenCompose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), KafkaExporterResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
}
