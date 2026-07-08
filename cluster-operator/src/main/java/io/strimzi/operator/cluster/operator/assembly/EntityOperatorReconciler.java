/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.CertSecretUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
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
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(EntityOperatorReconciler.class);
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
    private final boolean isEntityOperatorWatchedNamespaceEnabled;
    private final RoleOperator roleOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final boolean isCruiseControlEnabled;
    private final PodDisruptionBudgetOperator podDistruptionBudgetOperator;

    private String toCertificateHash = "";
    private String uoCertificateHash = "";
    private String toApiSecretHash = "";

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
        this.isEntityOperatorWatchedNamespaceEnabled = config.isEntityOperatorWatchedNamespaceEnabled();

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
                .compose(i -> topicOperatorSecret(clock))
                .compose(i -> userOperatorSecret(clock))
                .compose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> waitForDeploymentReadiness());
    }

    /**
     * Manages the Entity Operator Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> serviceAccount() {
        return VertxUtil.toFuture(serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                        shouldInstallEntityOperator() ? entityOperator.generateServiceAccount() : null
                )).mapEmpty();
    }

    /**
     * Manages the Entity Operator Role. This Role is always created when Topic or User operator is enabled and lives in
     * the same namespace as the Kafka cluster. Even if they watch some other namespace, this is used to access the CA
     * secrets.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> entityOperatorRole() {
        return VertxUtil.toFuture(roleOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                        shouldInstallEntityOperator() ? entityOperator.generateRole(reconciliation.namespace(), reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), EntityOperator.Permissions.BOTH) : null
                )).mapEmpty();
    }

    /**
     * Determines which operator permissions are needed for a given namespace.
     * This implements adaptive permission selection:
     * - Both TO and UO watch the namespace then use combined permissions (TO and UO permissions, with Secrets)
     * - Only TO watches then use TO-only permissions (no Secrets)
     * - Only UO watches then use UO-only permissions (with Secrets)
     *
     * @param namespace The namespace to determine permissions for
     * @return The OperatorType indicating which permissions to include
     */
    private EntityOperator.Permissions getPermissionsForNamespace(String namespace) {
        String toNamespace = entityOperator != null && entityOperator.topicOperator() != null
            ? entityOperator.topicOperator().watchedNamespace()
            : null;
        String uoNamespace = entityOperator != null && entityOperator.userOperator() != null
            ? entityOperator.userOperator().watchedNamespace()
            : null;

        boolean toWatches = toNamespace != null && toNamespace.equals(namespace) && topicOperatorHasValidConfig();
        boolean uoWatches = uoNamespace != null && uoNamespace.equals(namespace) && userOperatorHasValidConfig();

        if (toWatches && uoWatches) {
            // Both watch this namespace, use combined permissions
            return EntityOperator.Permissions.BOTH;
        } else if (toWatches) {
            // Only TO watches, use TO-only permissions (no Secrets)
            return EntityOperator.Permissions.TOPIC_OPERATOR;
        } else if (uoWatches) {
            // Only UO watches, use UO-only permissions (with Secrets)
            return EntityOperator.Permissions.USER_OPERATOR;
        } else {
            // This should never happen
            throw new IllegalStateException("Operator type determination called for namespace "
                    + namespace + " but neither operator watches it. " +
                    "Topic Operator namespace: " + toNamespace + ", User Operator namespace: " + uoNamespace);
        }
    }

    /**
     * Manages the Topic Operator Role. This Role is managed only when the Topic Operator is deployed and configured to
     * watch another namespace. It is created in the watched namespace.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> topicOperatorRole() {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            String namespace = entityOperator.topicOperator().watchedNamespace();
            Role role;

            if (!topicOperatorHasValidConfig()) {
                // Deletion case: delete the Role in watched namespace
                role = null;
            } else if (isEntityOperatorWatchedNamespaceEnabled && !namespace.equals(reconciliation.namespace())) {
                // Creation case: generate Role for watched namespace using adaptive permissions
                EntityOperator.Permissions permissions = getPermissionsForNamespace(namespace);
                role = entityOperator.generateRole(reconciliation.namespace(), namespace, KafkaResources.entityOperatorDeploymentName(reconciliation.name()), permissions);
            } else {
                // Feature disabled and no deletion needed (watchedNamespace = cluster namespace)
                return Future.succeededFuture();
            }

            // Only reconcile if namespace is different from cluster namespace
            if (namespace != null && !namespace.equals(reconciliation.namespace())) {
                return VertxUtil.toFuture(roleOperator
                        .reconcile(
                                reconciliation,
                                namespace,
                                KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                                role
                        )).mapEmpty();
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Manages the User Operator Role. This Role is managed only when the User Operator is deployed and configured to
     * watch another namespace. It is created in the watched namespace.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> userOperatorRole() {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            String namespace = entityOperator.userOperator().watchedNamespace();
            Role role;

            if (!userOperatorHasValidConfig()) {
                // Deletion case: delete the Role in watched namespace
                role = null;
            } else if (isEntityOperatorWatchedNamespaceEnabled && !namespace.equals(reconciliation.namespace())) {
                // Creation case: generate Role for watched namespace using adaptive permissions
                EntityOperator.Permissions permissions = getPermissionsForNamespace(namespace);
                role = entityOperator.generateRole(reconciliation.namespace(), namespace, KafkaResources.entityOperatorDeploymentName(reconciliation.name()), permissions);
            } else {
                // Feature disabled and no deletion needed (watchedNamespace = cluster namespace)
                return Future.succeededFuture();
            }

            // Only reconcile if namespace is different from cluster namespace
            if (namespace != null && !namespace.equals(reconciliation.namespace())) {
                return VertxUtil.toFuture(roleOperator
                        .reconcile(
                                reconciliation,
                                namespace,
                                KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                                role
                        )).mapEmpty();
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Manages the Topic Operator Role Bindings. One Role Binding is in the namespace where the Kafka cluster exists and
     * where the deployment runs. If the operator is configured to watch another namespace, another Role Binding is
     * created for that namespace as well.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> topicOperatorRoleBindings() {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            String watchedNamespace = entityOperator.topicOperator().watchedNamespace();
            RoleBinding ownNamespaceRoleBinding;
            RoleBinding watchedNamespaceRoleBinding;

            if (!entityOperatorHasValidConfig()) {
                // Deletion case: entire deployment being deleted, delete both RoleBindings
                ownNamespaceRoleBinding = null;
                watchedNamespaceRoleBinding = null;
            } else {
                // Creation case: generate RoleBindings
                ownNamespaceRoleBinding = entityOperator.topicOperator().generateRoleBindingForRole(reconciliation.namespace(), reconciliation.namespace());
                watchedNamespaceRoleBinding = isEntityOperatorWatchedNamespaceEnabled
                        ? entityOperator.topicOperator().generateRoleBindingForRole(reconciliation.namespace(), watchedNamespace)
                        : null;
            }

            // Always reconcile own namespace RoleBinding
            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture = VertxUtil.toFuture(roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(),
                            KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()),
                            ownNamespaceRoleBinding));

            // Reconcile watched namespace RoleBinding if different from cluster namespace
            Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;
            if (watchedNamespace != null && !watchedNamespace.equals(reconciliation.namespace())) {
                watchedNamespaceFuture = VertxUtil.toFuture(roleBindingOperator
                        .reconcile(reconciliation, watchedNamespace,
                                KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()),
                                watchedNamespaceRoleBinding));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            return Future.join(ownNamespaceFuture, watchedNamespaceFuture).mapEmpty();
        } else {
            return VertxUtil.toFuture(roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()), null))
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
        if (entityOperator != null && entityOperator.userOperator() != null) {
            String watchedNamespace = entityOperator.userOperator().watchedNamespace();
            RoleBinding ownNamespaceRoleBinding;
            RoleBinding watchedNamespaceRoleBinding;

            if (!entityOperatorHasValidConfig()) {
                // Deletion case: entire deployment being deleted, delete both RoleBindings
                ownNamespaceRoleBinding = null;
                watchedNamespaceRoleBinding = null;
            } else {
                // Creation case: generate RoleBindings
                ownNamespaceRoleBinding = entityOperator.userOperator().generateRoleBindingForRole(reconciliation.namespace(), reconciliation.namespace());
                watchedNamespaceRoleBinding = isEntityOperatorWatchedNamespaceEnabled
                        ? entityOperator.userOperator().generateRoleBindingForRole(reconciliation.namespace(), watchedNamespace)
                        : null;
            }

            // Always reconcile own namespace RoleBinding
            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture = VertxUtil.toFuture(roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(),
                            KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()),
                            ownNamespaceRoleBinding));

            // Reconcile watched namespace RoleBinding if different from cluster namespace
            Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;
            if (watchedNamespace != null && !watchedNamespace.equals(reconciliation.namespace())) {
                watchedNamespaceFuture = VertxUtil.toFuture(roleBindingOperator
                        .reconcile(reconciliation, watchedNamespace,
                                KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()),
                                watchedNamespaceRoleBinding));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            return Future.join(ownNamespaceFuture, watchedNamespaceFuture).mapEmpty();
        } else {
            return VertxUtil.toFuture(roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()), null))
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
        if (shouldInstallEntityOperator() && entityOperator.topicOperator() != null) {
            return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, entityOperator.topicOperator().logging(), null)
                    .compose(logging ->
                            VertxUtil.toFuture(configMapOperator.reconcile(
                                    reconciliation,
                                    reconciliation.namespace(),
                                    KafkaResources.entityTopicOperatorLoggingConfigMapName(reconciliation.name()),
                                    entityOperator.topicOperator().generateMetricsAndLogConfigMap(logging)
                            ))
                    ).mapEmpty();
        } else {
            return VertxUtil.toFuture(configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorLoggingConfigMapName(reconciliation.name()), null))
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
        if (shouldInstallEntityOperator() && entityOperator.userOperator() != null) {
            return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, entityOperator.userOperator().logging(), null)
                    .compose(logging ->
                            VertxUtil.toFuture(configMapOperator.reconcile(
                                    reconciliation,
                                    reconciliation.namespace(),
                                    KafkaResources.entityUserOperatorLoggingConfigMapName(reconciliation.name()),
                                    entityOperator.userOperator().generateMetricsAndLogConfigMap(logging)
                            ))
                    ).mapEmpty();
        } else {
            return VertxUtil.toFuture(configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorLoggingConfigMapName(reconciliation.name()), null))
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
        if (shouldInstallEntityOperator() && entityOperator.topicOperator() != null) {
            return VertxUtil.toFuture(secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name())))
                    .compose(oldSecret -> {
                        Secret newSecret = entityOperator.topicOperator().generateCertificatesSecret(clusterCa, oldSecret, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                        return VertxUtil.toFuture(secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()), newSecret))
                                .compose(i -> {
                                    toCertificateHash = CertSecretUtils.getCertificateShortThumbprint(newSecret, Ca.SecretEntry.CRT.asKey(EntityOperator.COMPONENT_TYPE));

                                    return Future.succeededFuture();
                                });
                    })
                    .compose(i -> {
                        if (isCruiseControlEnabled) {
                            return VertxUtil.toFuture(secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityTopicOperatorCcApiSecretName(reconciliation.name())))
                                    .compose(secret -> {
                                        toApiSecretHash = ReconcilerUtils.hashSecretContent(secret);
                                        return Future.succeededFuture();
                                    });
                        }
                        return Future.succeededFuture();
                    });
        } else {
            return VertxUtil.toFuture(secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()), null))
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
        if (shouldInstallEntityOperator() && entityOperator.userOperator() != null) {
            return VertxUtil.toFuture(secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name())))
                    .compose(oldSecret -> {
                        Secret newSecret = entityOperator.userOperator().generateCertificatesSecret(clusterCa, oldSecret, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                        return VertxUtil.toFuture(secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()), newSecret))
                                .compose(i -> {
                                    uoCertificateHash = CertSecretUtils.getCertificateShortThumbprint(newSecret, Ca.SecretEntry.CRT.asKey(EntityOperator.COMPONENT_TYPE));

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return VertxUtil.toFuture(secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()), null))
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
            return VertxUtil.toFuture(networkPolicyOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                            shouldInstallEntityOperator() ? entityOperator.generateNetworkPolicy() : null
                    )).mapEmpty();
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
            return VertxUtil.toFuture(podDistruptionBudgetOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                            shouldInstallEntityOperator() ? entityOperator.generatePodDisruptionBudget() : null
                    )).mapEmpty();
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
        if (shouldInstallEntityOperator()) {
            Map<String, String> podAnnotations = new LinkedHashMap<>();
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCa.caCertGeneration()));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()));
            podAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, toCertificateHash + uoCertificateHash);

            if (entityOperator.topicOperator() != null && isCruiseControlEnabled) {
                podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, toApiSecretHash);
            }

            Deployment deployment = entityOperator.generateDeployment(podAnnotations, isOpenShift, imagePullPolicy, imagePullSecrets);

            return VertxUtil.toFuture(deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), deployment))
                    .mapEmpty();
        } else {
            // Log warning and fail if we're deleting due to invalid configuration
            if (entityOperator != null && !entityOperatorHasValidConfig()) {
                String errorMessage = "Entity Operator deployment deleted because Topic Operator and/or User Operator are configured with " +
                        "watchedNamespace set to a different namespace but the feature is disabled. " +
                        "To enable cross-namespace watching, set STRIMZI_ENTITY_OPERATOR_WATCHED_NAMESPACE_ENABLED=true";

                LOGGER.warnCr(reconciliation, errorMessage);

                return VertxUtil.toFuture(deploymentOperator
                        .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), null))
                        .compose(v -> Future.failedFuture(new InvalidConfigurationException(errorMessage)));
            }

            return VertxUtil.toFuture(deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), null))
                    .mapEmpty();
        }
    }

    /**
     * Waits for the Entity Operator deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> waitForDeploymentReadiness() {
        if (shouldInstallEntityOperator()) {
            return VertxUtil.toFuture(deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), 1_000, operationTimeoutMs))
                    .compose(i -> VertxUtil.toFuture(deploymentOperator.readiness(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), 1_000, operationTimeoutMs)));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Determines if Topic Operator has a valid configuration.
     * Configuration is valid when:
     * - Feature is enabled (any watchedNamespace is allowed), OR
     * - Feature is disabled AND watchedNamespace equals cluster namespace
     *
     * @return true if configuration is valid, false otherwise
     */
    private boolean topicOperatorHasValidConfig() {
        if (entityOperator == null || entityOperator.topicOperator() == null) {
            return true;
        }

        return isEntityOperatorWatchedNamespaceEnabled
                || entityOperator.topicOperator().watchedNamespace().equals(reconciliation.namespace());
    }

    /**
     * Determines if User Operator has a valid configuration.
     * Configuration is valid when:
     * - Feature is enabled (any watchedNamespace is allowed), OR
     * - Feature is disabled AND watchedNamespace equals cluster namespace
     *
     * @return true if configuration is valid, false otherwise
     */
    private boolean userOperatorHasValidConfig() {
        if (entityOperator == null || entityOperator.userOperator() == null) {
            return true;
        }

        return isEntityOperatorWatchedNamespaceEnabled
                || entityOperator.userOperator().watchedNamespace().equals(reconciliation.namespace());
    }

    /**
     * Determines if Entity Operator has a valid configuration.
     * Both Topic Operator and User Operator must have valid configurations.
     *
     * @return true if Entity Operator configuration is valid, false otherwise
     */
    private boolean entityOperatorHasValidConfig() {
        return topicOperatorHasValidConfig() && userOperatorHasValidConfig();
    }

    /**
     * Determines if the Entity Operator should be installed.
     *
     * @return true if Entity Operator should be installed, false otherwise
     */
    private boolean shouldInstallEntityOperator() {
        return entityOperator != null && entityOperatorHasValidConfig();
    }

    /**
     * Determines if the Topic Operator should be installed.
     *
     * @return true if Topic Operator should be installed, false otherwise
     */
    private boolean shouldInstallTopicOperator() {
        return entityOperator != null && entityOperator.topicOperator() != null
                && topicOperatorHasValidConfig();
    }

    /**
     * Determines if the User Operator should be installed.
     *
     * @return true if User Operator should be installed, false otherwise
     */
    private boolean shouldInstallUserOperator() {
        return entityOperator != null && entityOperator.userOperator() != null
                && userOperatorHasValidConfig();
    }
}
