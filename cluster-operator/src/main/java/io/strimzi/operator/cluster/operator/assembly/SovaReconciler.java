/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.sova.SovaResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.CertSecretUtils;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.Sova;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
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
 * Class used for reconciliation of Sova. This class contains both the steps of the Sova
 * reconciliation pipeline and is also used to store the state between them.
 */
public class SovaReconciler {
    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final Sova sova;
    private final Ca clusterCa;
    private final List<String> maintenanceWindows;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isPodDisruptionBudgetGeneration;
    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final RoleOperator roleOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final ConfigMapOperator configMapOperator;

    private String certificateHash = "";

    /**
     * Constructs the Sova reconciler.
     *
     * @param reconciliation    Reconciliation marker
     * @param config            Cluster Operator Configuration
     * @param supplier          Supplier with Kubernetes Resource Operators
     * @param kafkaAssembly     The Kafka custom resource
     * @param versions          The supported Kafka versions
     * @param clusterCa         The Cluster CA instance
     * @param securityContext   Kafka cluster security context
     */
    public SovaReconciler(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            Ca clusterCa,
            KafkaClusterSecurityContext securityContext) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.sova = Sova.fromCrd(reconciliation, kafkaAssembly, versions, supplier.sharedEnvironmentProvider, securityContext);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.isPodDisruptionBudgetGeneration = config.isPodDisruptionBudgetGeneration();

        this.deploymentOperator = supplier.deploymentOperations;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.roleOperator = supplier.roleOperations;
        this.roleBindingOperator = supplier.roleBindingOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.configMapOperator = supplier.configMapOperations;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     * @param clock             The clock for supplying the reconciler with the time instant of each reconciliation cycle
     *
     * @return CompletionStage which completes when the reconciliation completes
     */
    public CompletionStage<Void> reconcile(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, Clock clock) {
        return serviceAccount()
                .thenCompose(i -> role())
                .thenCompose(i -> roleBinding())
                .thenCompose(i -> loggingConfigMap())
                .thenCompose(i -> certificatesSecret(clock))
                .thenCompose(i -> networkPolicy())
                .thenCompose(i -> podDisruptionBudget())
                .thenCompose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .thenCompose(i -> waitForDeploymentReadiness());
    }

    private CompletionStage<Void> serviceAccount() {
        return serviceAccountOperator.reconcile(
                reconciliation,
                reconciliation.namespace(),
                SovaResources.serviceAccountName(reconciliation.name()),
                sova != null ? sova.generateServiceAccount() : null
        ).thenApply(i -> null);
    }

    private CompletionStage<Void> role() {
        return roleOperator.reconcile(
                reconciliation,
                reconciliation.namespace(),
                SovaResources.componentName(reconciliation.name()),
                sova != null ? sova.generateRole() : null
        ).thenApply(i -> null);
    }

    private CompletionStage<Void> roleBinding() {
        return roleBindingOperator.reconcile(
                reconciliation,
                reconciliation.namespace(),
                SovaResources.roleBindingName(reconciliation.name()),
                sova != null ? sova.generateRoleBindingForRole() : null
        ).thenApply(i -> null);
    }

    private CompletionStage<Void> loggingConfigMap() {
        if (sova != null) {
            return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, sova.logging(), null)
                    .thenCompose(metricsAndLogging -> {
                        ConfigMap configMap = sova.generateMetricsAndLogConfigMap(metricsAndLogging);
                        return configMapOperator.reconcile(
                                reconciliation,
                                reconciliation.namespace(),
                                SovaResources.configMapName(reconciliation.name()),
                                configMap
                        ).thenApply(i -> null);
                    });
        } else {
            return configMapOperator.reconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    SovaResources.configMapName(reconciliation.name()),
                    null
            ).thenApply(i -> null);
        }
    }

    private CompletionStage<Void> certificatesSecret(Clock clock) {
        if (sova != null) {
            return secretOperator.getAsync(reconciliation.namespace(), SovaResources.secretName(reconciliation.name()))
                    .thenCompose(oldSecret -> sova.generateCertificatesSecret(clusterCa, oldSecret,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())))
                    .thenCompose(newSecret -> secretOperator.reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            SovaResources.secretName(reconciliation.name()),
                            newSecret
                    ).thenApply(result -> {
                        certificateHash = CertSecretUtils.getCertificateShortThumbprint(newSecret,
                                Ca.SecretEntry.CRT.asKey(Sova.COMPONENT_TYPE));
                        return null;
                    }));
        } else {
            return CompletableFuture.allOf(
                    clusterCa.cleanupEndEntityCert(SovaResources.secretName(reconciliation.name())).toCompletableFuture(),
                    secretOperator.reconcile(reconciliation, reconciliation.namespace(), SovaResources.secretName(reconciliation.name()), null).toCompletableFuture()
            ).thenApply(i -> null);
        }
    }

    protected CompletionStage<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    SovaResources.componentName(reconciliation.name()),
                    sova != null ? sova.generateNetworkPolicy() : null
            ).thenApply(i -> null);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    protected CompletionStage<Void> podDisruptionBudget() {
        if (isPodDisruptionBudgetGeneration) {
            return podDisruptionBudgetOperator.reconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    SovaResources.componentName(reconciliation.name()),
                    sova != null ? sova.generatePodDisruptionBudget() : null
            ).thenApply(i -> null);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletionStage<Void> deployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (sova != null) {
            Map<String, String> podAnnotations = new LinkedHashMap<>();
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCa.caCertGeneration()));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()));
            podAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, certificateHash);

            Deployment deployment = sova.generateDeployment(podAnnotations, isOpenShift, imagePullPolicy, imagePullSecrets);
            return deploymentOperator.reconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    SovaResources.componentName(reconciliation.name()),
                    deployment
            ).thenApply(i -> null);
        } else {
            return deploymentOperator.reconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    SovaResources.componentName(reconciliation.name()),
                    null
            ).thenApply(i -> null);
        }
    }

    private CompletionStage<Void> waitForDeploymentReadiness() {
        if (sova != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), SovaResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .thenCompose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), SovaResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
}
