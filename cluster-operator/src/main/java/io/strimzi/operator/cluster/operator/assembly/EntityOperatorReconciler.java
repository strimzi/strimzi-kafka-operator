/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class used for reconciliation of Entity Operator. This class contains both the steps of the Entity Operator
 * reconciliation pipeline and is also used to store the state between them.
 */
public class EntityOperatorReconciler {
    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final EntityOperator entityOperator;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;

    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isPodDisruptionBudgetGeneration;
    private final RoleOperator roleOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final boolean isCruiseControlEnabled;
    private final PodDisruptionBudgetOperator podDistruptionBudgetOperator;

    private String toCertificateHash = "";
    private String uoCertificateHash = "";
    private String ccApiSecretHash = "";

    /**
     * Constructs the Entity Operator reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param kafkaAssembly             The Kafka custom resource
     * @param clusterCa                 The Cluster CA instance
     */
    public EntityOperatorReconciler(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Kafka kafkaAssembly,
            ClusterCa clusterCa
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.entityOperator = EntityOperator.fromCrd(reconciliation, kafkaAssembly, supplier.sharedEnvironmentProvider, config);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.isCruiseControlEnabled = kafkaAssembly.getSpec().getCruiseControl() != null;
        this.isPodDisruptionBudgetGeneration = config.isPodDisruptionBudgetGeneration();
        
        this.deploymentOperator = supplier.deploymentOperations;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.roleOperator = supplier.roleOperations;
        this.roleBindingOperator = supplier.roleBindingOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDistruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
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
                .compose(i -> entityOperatorRole())
                .compose(i -> topicOperatorRole())
                .compose(i -> userOperatorRole())
                .compose(i -> networkPolicy())
                .compose(i -> podDistruptionBudget())
                .compose(i -> topicOperatorRoleBindings())
                .compose(i -> userOperatorRoleBindings())
                .compose(i -> topicOperatorConfigMap())
                .compose(i -> userOperatorConfigMap())
                .compose(i -> topicOperatorCruiseControlApiSecret())
                .compose(i -> topicOperatorSecret(clock))
                .compose(i -> userOperatorSecret(clock))
                .compose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> waitForDeploymentReadiness());
    }

    /**
     * If not present, we generate a new Topic Operator secret containing its admin credentials for the Cruise Control API secret.
     * We also generate the secret's content hash, that is later used to detect changes that require a pod restart.
     *
     * @return Future which completes when the reconciliation is done.
     */
    protected Future<Void> topicOperatorCruiseControlApiSecret() {
        if (isCruiseControlEnabled) {
            String ccApiSecretName = KafkaResources.entityTopicOperatorCcApiSecretName(reconciliation.name());
            if (entityOperator != null && entityOperator.topicOperator() != null) {
                return secretOperator.getAsync(reconciliation.namespace(), ccApiSecretName)
                    .compose(oldSecret -> {
                        Secret newSecret = entityOperator.topicOperator().generateCruiseControlApiSecret(oldSecret);
                        this.ccApiSecretHash = ReconcilerUtils.hashSecretContent(newSecret);
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), ccApiSecretName, newSecret)
                            .mapEmpty();
                    });
            } else {
                return secretOperator.reconcile(reconciliation, reconciliation.namespace(), ccApiSecretName, null)
                    .mapEmpty();
            }
        }
        return Future.succeededFuture();
    }

    /**
     * Manages the Entity Operator Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                        entityOperator != null ? entityOperator.generateServiceAccount() : null
                ).mapEmpty();
    }

    /**
     * Manages the Entity Operator Role. This Role is always created when Topic or User operator is enabled and lives in
     * the same namespace as the Kafka cluster. Even if they watch some other namespace, this is used to access the CA
     * secrets.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> entityOperatorRole() {
        return roleOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                        entityOperator != null ? entityOperator.generateRole(reconciliation.namespace(), reconciliation.namespace()) : null
                ).mapEmpty();
    }

    /**
     * Manages the Topic Operator Role. This Role is managed only when the Topic Operator is deployed and configured to
     * watch another namespace. It is created in the watched namespace.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> topicOperatorRole() {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            String watchedNamespace = entityOperator.topicOperator().watchedNamespace();

            if (!watchedNamespace.equals(reconciliation.namespace())) {
                return roleOperator
                        .reconcile(
                                reconciliation,
                                watchedNamespace,
                                KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                                entityOperator.generateRole(reconciliation.namespace(), watchedNamespace)
                        ).mapEmpty();
            } else {
                return Future.succeededFuture();
            }
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Manages the User Operator Role. This Role is managed only when the User Operator is deployed and configured to
     * watch another namespace. It is created in the watched namespace.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> userOperatorRole() {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            String watchedNamespace = entityOperator.userOperator().watchedNamespace();

            if (!watchedNamespace.equals(reconciliation.namespace())) {
                return roleOperator
                        .reconcile(
                                reconciliation,
                                watchedNamespace,
                                KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                                entityOperator.generateRole(reconciliation.namespace(), watchedNamespace)
                        ).mapEmpty();
            } else {
                return Future.succeededFuture();
            }
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Manages the Topic Operator Role Bindings. One Role Binding is in the namespace where the Kafka cluster exists and
     * where the deployment runs. If the operator is configured to watch another namespace, another Role Binding is
     * created for that namespace as well.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> topicOperatorRoleBindings() {
        if (entityOperator != null && entityOperator.topicOperator() != null)   {
            String watchedNamespace = entityOperator.topicOperator().watchedNamespace();

            Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;
            if (!watchedNamespace.equals(reconciliation.namespace()))    {
                watchedNamespaceFuture = roleBindingOperator.reconcile(reconciliation, watchedNamespace,
                        KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()), entityOperator.topicOperator().generateRoleBindingForRole(reconciliation.namespace(), watchedNamespace));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture = roleBindingOperator.reconcile(reconciliation, reconciliation.namespace(),
                    KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()), entityOperator.topicOperator().generateRoleBindingForRole(reconciliation.namespace(), reconciliation.namespace()));

            return Future.join(ownNamespaceFuture, watchedNamespaceFuture)
                    .mapEmpty();
        } else {
            return roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the User Operator Role Bindings. One Role Binding is in the namespace where the Kafka cluster exists and
     * where the deployment runs. If the operator is configured to watch another namespace, another Role Binding is
     * created for that namespace as well.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> userOperatorRoleBindings() {
        if (entityOperator != null && entityOperator.userOperator() != null)   {
            String watchedNamespace = entityOperator.userOperator().watchedNamespace();

            Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;
            if (!watchedNamespace.equals(reconciliation.namespace()))    {
                watchedNamespaceFuture = roleBindingOperator.reconcile(reconciliation, watchedNamespace,
                        KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()), entityOperator.userOperator().generateRoleBindingForRole(reconciliation.namespace(), watchedNamespace));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture = roleBindingOperator.reconcile(reconciliation, reconciliation.namespace(),
                    KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()), entityOperator.userOperator().generateRoleBindingForRole(reconciliation.namespace(), reconciliation.namespace()));

            return Future.join(ownNamespaceFuture, watchedNamespaceFuture)
                    .mapEmpty();
        } else {
            return roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the Topic Operator Config Map with logging configuration (Topic Operator does not have any metrics
     * configuration).
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> topicOperatorConfigMap() {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, entityOperator.topicOperator().logging(), null)
                    .compose(logging ->
                            configMapOperator.reconcile(
                                    reconciliation,
                                    reconciliation.namespace(),
                                    KafkaResources.entityTopicOperatorLoggingConfigMapName(reconciliation.name()),
                                    entityOperator.topicOperator().generateMetricsAndLogConfigMap(logging)
                            )
                    ).mapEmpty();
        } else {
            return configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorLoggingConfigMapName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the USer Operator Config Map with logging configuration (User Operator does not have any metrics
     * configuration).
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> userOperatorConfigMap() {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, entityOperator.userOperator().logging(), null)
                    .compose(logging ->
                            configMapOperator.reconcile(
                                    reconciliation,
                                    reconciliation.namespace(),
                                    KafkaResources.entityUserOperatorLoggingConfigMapName(reconciliation.name()),
                                    entityOperator.userOperator().generateMetricsAndLogConfigMap(logging)
                            )
                    ).mapEmpty();
        } else {
            return configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorLoggingConfigMapName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the Topic Operator certificates Secret.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Future which completes when the reconciliation is done
     */
    protected Future<Void> topicOperatorSecret(Clock clock) {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        Secret newSecret = entityOperator.topicOperator().generateCertificatesSecret(clusterCa, oldSecret, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()), newSecret)
                                .compose(i -> {
                                    toCertificateHash = CertUtils.getCertificateShortThumbprint(newSecret, Ca.SecretEntry.CRT.asKey(EntityOperator.COMPONENT_TYPE));

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the User Operator certificates Secret.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Future which completes when the reconciliation is done
     */
    protected Future<Void> userOperatorSecret(Clock clock) {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        Secret newSecret = entityOperator.userOperator().generateCertificatesSecret(clusterCa, oldSecret, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()), newSecret)
                                .compose(i -> {
                                    uoCertificateHash = CertUtils.getCertificateShortThumbprint(newSecret, Ca.SecretEntry.CRT.asKey(EntityOperator.COMPONENT_TYPE));

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }
    /**
     * Manages the Entity Operator Network Policies.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                            entityOperator != null ? entityOperator.generateNetworkPolicy() : null
                    ).mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }
    /**
     * Manages the Entity Operator Pod Disruption Budget
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> podDistruptionBudget() {
        if (isPodDisruptionBudgetGeneration) {
            return podDistruptionBudgetOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                            entityOperator != null ? entityOperator.generatePodDisruptionBudget() : null
                    ).mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }
    /**
     * Manages the Entity Operator Deployment.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> deployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (entityOperator != null) {
            Map<String, String> podAnnotations = new LinkedHashMap<>();
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCa.caCertGeneration()));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()));
            podAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, toCertificateHash + uoCertificateHash);

            if (entityOperator.topicOperator() != null && isCruiseControlEnabled) {
                podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, ccApiSecretHash);
            }

            Deployment deployment = entityOperator.generateDeployment(podAnnotations, isOpenShift, imagePullPolicy, imagePullSecrets);


            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), deployment)
                    .mapEmpty();
        } else  {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Waits for the Entity Operator deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> waitForDeploymentReadiness() {
        if (entityOperator != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
