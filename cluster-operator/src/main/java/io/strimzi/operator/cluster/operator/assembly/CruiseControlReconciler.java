/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.Future;

import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

/**
 * Class used for reconciliation of Cruise Control. This class contains both the steps of the Cruise Control
 * reconciliation pipeline and is also used to store the state between them.
 */
public class CruiseControlReconciler {
    private final Reconciliation reconciliation;
    private final CruiseControl cruiseControl;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;
    private final long operationTimeoutMs;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final boolean isNetworkPolicyGeneration;

    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ServiceOperator serviceOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final ConfigMapOperator configMapOperator;

    private boolean existingCertsChanged = false;

    /**
     * Constructs the Cruise Control reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafkaAssembly             The Kafka custom resource
     * @param versions                  The supported Kafka versions
     * @param storage                   The actual storage configuration used by the cluster. This might differ from the
     *                                  storage configuration configured by the user in the Kafka CR due to un-allowed changes.
     * @param clusterCa                 The Cluster CA instance
     * @param operationTimeoutMs        Timeout for Kubernetes operations
     * @param operatorNamespace         Namespace where the Cluster Operator is running (used to generate network policies)
     * @param operatorNamespaceLabels   Labels of the namespace where the Cluster Operator is running (used to generate network policies)
     * @param isNetworkPolicyGeneration Flag indicating whether network policies should be generated or not
     * @param deploymentOperator        The Deployment operator for working with Kubernetes Deployments
     * @param secretOperator            The Secret operator for working with Kubernetes Secrets
     * @param serviceAccountOperator    The Service Account operator for working with Kubernetes Service Accounts
     * @param serviceOperator           The Service operator for working with Kubernetes Service Accounts
     * @param networkPolicyOperator     The Network Policy operator for working with Kubernetes Service Accounts
     * @param configMapOperator         The Config Map operator for working with Kubernetes Config Maps
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public CruiseControlReconciler(
            Reconciliation reconciliation,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            Storage storage,
            ClusterCa clusterCa,
            long operationTimeoutMs,
            String operatorNamespace,
            Labels operatorNamespaceLabels,
            boolean isNetworkPolicyGeneration,
            DeploymentOperator deploymentOperator,
            SecretOperator secretOperator,
            ServiceAccountOperator serviceAccountOperator,
            ServiceOperator serviceOperator,
            NetworkPolicyOperator networkPolicyOperator,
            ConfigMapOperator configMapOperator
    ) {
        this.reconciliation = reconciliation;
        this.cruiseControl = CruiseControl.fromCrd(reconciliation, kafkaAssembly, versions, storage);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.operationTimeoutMs = operationTimeoutMs;
        this.operatorNamespace = operatorNamespace;
        this.operatorNamespaceLabels = operatorNamespaceLabels;
        this.isNetworkPolicyGeneration = isNetworkPolicyGeneration;

        this.deploymentOperator = deploymentOperator;
        this.secretOperator = secretOperator;
        this.serviceAccountOperator = serviceAccountOperator;
        this.serviceOperator = serviceOperator;
        this.networkPolicyOperator = networkPolicyOperator;
        this.configMapOperator = configMapOperator;
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
        return cruiseControlNetworkPolicy()
                .compose(i -> cruiseControlServiceAccount())
                .compose(i -> cruiseControlLoggingAndMetricsConfigMap())
                .compose(i -> cruiseControlCertificatesSecret(dateSupplier))
                .compose(i -> cruiseControlApiSecret())
                .compose(i -> cruiseControlService())
                .compose(i -> cruiseControlDeployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> cruiseControlReady());
    }

    /**
     * Manages the Cruise Control Network Policies.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlNetworkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            CruiseControlResources.networkPolicyName(reconciliation.name()),
                            cruiseControl != null ? cruiseControl.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels) : null
                    ).map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Manages the Cruise Control Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlServiceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        CruiseControlResources.serviceAccountName(reconciliation.name()),
                        cruiseControl != null ? cruiseControl.generateServiceAccount() : null
                ).map((Void) null);
    }

    /**
     * Manages the Cruise Control Config Map with logging and metrics configuration.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlLoggingAndMetricsConfigMap() {
        if (cruiseControl != null)  {
            return Util.metricsAndLogging(reconciliation, configMapOperator, reconciliation.namespace(), cruiseControl.getLogging(), cruiseControl.getMetricsConfigInCm())
                    .compose(metricsAndLogging -> {
                        ConfigMap logAndMetricsConfigMap = cruiseControl.generateMetricsAndLogConfigMap(metricsAndLogging);

                        return configMapOperator
                                .reconcile(
                                        reconciliation,
                                        reconciliation.namespace(),
                                        CruiseControlResources.logAndMetricsConfigMapName(reconciliation.name()),
                                        logAndMetricsConfigMap
                                ).map((Void) null);
                    });
        } else {
            return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.logAndMetricsConfigMapName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the Cruise Control certificates Secret.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlCertificatesSecret(Supplier<Date> dateSupplier) {
        if (cruiseControl != null) {
            return secretOperator.getAsync(reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()),
                                        cruiseControl.generateCertificatesSecret(reconciliation.namespace(), reconciliation.name(), clusterCa, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier)))
                                .compose(patchResult -> {
                                    if (patchResult instanceof ReconcileResult.Patched) {
                                        // The secret is patched and some changes to the existing certificates actually occurred
                                        existingCertsChanged = ModelUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
                                    } else {
                                        existingCertsChanged = false;
                                    }

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the Cruise Control API keys secret.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlApiSecret() {
        if (cruiseControl != null) {
            return secretOperator.getAsync(reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        Secret newSecret = cruiseControl.generateApiSecret();

                        if (oldSecret != null)  {
                            // The credentials should not change with every release
                            // So if the secret with credentials already exists, we re-use the values
                            // But we use the new secret to update labels etc. if needed
                            newSecret.setData(oldSecret.getData());
                        }

                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()), newSecret)
                                .map((Void) null);
                    });
        } else {
            return secretOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the Cruise Control Service.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlService() {
        return serviceOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        CruiseControlResources.serviceName(reconciliation.name()),
                        cruiseControl != null ? cruiseControl.generateService() : null
                ).map((Void) null);
    }

    /**
     * Manages the Cruise Control Deployment.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (cruiseControl != null) {
            Deployment deployment = cruiseControl.generateDeployment(isOpenShift, imagePullPolicy, imagePullSecrets);

            int caCertGeneration = ModelUtils.caCertGeneration(clusterCa);
            Annotations.annotations(deployment.getSpec().getTemplate()).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.deploymentName(reconciliation.name()), deployment)
                    .compose(patchResult -> {
                        if (patchResult instanceof ReconcileResult.Noop)   {
                            // Deployment needs ot be rolled because the certificate secret changed
                            if (existingCertsChanged) {
                                return cruiseControlRollingUpdate();
                            }
                        }

                        // No need to roll, we patched the deployment (and it will roll itself) or we created a new one
                        return Future.succeededFuture();
                    });
        } else {
            return deploymentOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.deploymentName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Triggers the rolling update of the Cruise Control. This is used to trigger the roll when the certificates change.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlRollingUpdate() {
        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), CruiseControlResources.deploymentName(reconciliation.name()), operationTimeoutMs);
    }

    /**
     * Waits for the Cruise Control deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> cruiseControlReady() {
        if (cruiseControl != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), CruiseControlResources.deploymentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), CruiseControlResources.deploymentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
