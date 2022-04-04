/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

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
    private final RoleOperator roleOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final ConfigMapOperator configMapOperator;

    private boolean existingEntityTopicOperatorCertsChanged = false;
    private boolean existingEntityUserOperatorCertsChanged = false;

    /**
     * Constructs the Entity Operator reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param operationTimeoutMs        Timeout for Kubernetes operations
     * @param kafkaAssembly             The Kafka custom resource
     * @param versions                  The supported Kafka versions
     * @param clusterCa                 The Cluster CA instance
     * @param deploymentOperator        The Deployment operator for working with Kubernetes Deployments
     * @param secretOperator            The Secret operator for working with Kubernetes Secrets
     * @param serviceAccountOperator    The Service Account operator for working with Kubernetes Service Accounts
     * @param roleOperator              The Role operator for working with Kubernetes Roles
     * @param roleBindingOperator       The Role Binding operator for working with Kubernetes Role Bindings
     * @param configMapOperator         The Config Map operator for working with Kubernetes Config Maps
     */
    public EntityOperatorReconciler(
            Reconciliation reconciliation,
            long operationTimeoutMs,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            ClusterCa clusterCa,
            DeploymentOperator deploymentOperator,
            SecretOperator secretOperator,
            ServiceAccountOperator serviceAccountOperator,
            RoleOperator roleOperator,
            RoleBindingOperator roleBindingOperator,
            ConfigMapOperator configMapOperator
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = operationTimeoutMs;
        this.entityOperator = EntityOperator.fromCrd(reconciliation, kafkaAssembly, versions);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();

        this.deploymentOperator = deploymentOperator;
        this.secretOperator = secretOperator;
        this.serviceAccountOperator = serviceAccountOperator;
        this.roleOperator = roleOperator;
        this.roleBindingOperator = roleBindingOperator;
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
        return reconcileServiceAccount()
                .compose(i -> reconcileEntityOperatorRole())
                .compose(i -> recocnileTopicOperatorRole())
                .compose(i -> reconcileUserOperatorRole())
                .compose(i -> reconcileTopicOperatorRoleBindings())
                .compose(i -> reconcileUserOperatorRoleBindings())
                .compose(i -> reconcileTopicOperagorConfigMap())
                .compose(i -> reconcileUserOperatorConfigMap())
                .compose(i -> deleteOldEntityOperatorSecret())
                .compose(i -> reconcileTopicOperatorSecret(dateSupplier))
                .compose(i -> reconcileUserOperatorSecret(dateSupplier))
                .compose(i -> reconcileDeployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> waitForDeploymentReadiness());
    }

    /**
     * Manages the Entity Operator Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileServiceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                        entityOperator != null ? entityOperator.generateServiceAccount() : null
                ).map((Void) null);
    }

    /**
     * Manages the Entity Operator Role. This Role is always created when Topic or User operator is enabled and lives in
     * the same namespace as the Kafka cluster. Even if they watch some other namespace, this is used to access the CA
     * secrets.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileEntityOperatorRole() {
        return roleOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                        entityOperator != null ? entityOperator.generateRole(reconciliation.namespace(), reconciliation.namespace()) : null
                ).map((Void) null);
    }

    /**
     * Manages the Topic Operator Role. This Role is managed only when the Topic Operator is deployed and configured to
     * watch another namespace. It is created in the watched namespace.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> recocnileTopicOperatorRole() {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            String watchedNamespace = entityOperator.topicOperator().watchedNamespace();

            if (!watchedNamespace.equals(reconciliation.namespace())) {
                return roleOperator
                        .reconcile(
                                reconciliation,
                                watchedNamespace,
                                KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                                entityOperator.generateRole(reconciliation.namespace(), watchedNamespace)
                        ).map((Void) null);
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
    Future<Void> reconcileUserOperatorRole() {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            String watchedNamespace = entityOperator.userOperator().watchedNamespace();

            if (!watchedNamespace.equals(reconciliation.namespace())) {
                return roleOperator
                        .reconcile(
                                reconciliation,
                                watchedNamespace,
                                KafkaResources.entityOperatorDeploymentName(reconciliation.name()),
                                entityOperator.generateRole(reconciliation.namespace(), watchedNamespace)
                        ).map((Void) null);
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
    Future<Void> reconcileTopicOperatorRoleBindings() {
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

            return CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture)
                    .map((Void) null);
        } else {
            return roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorRoleBinding(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the User Operator Role Bindings. One Role Binding is in the namespace where the Kafka cluster exists and
     * where the deployment runs. If the operator is configured to watch another namespace, another Role Binding is
     * created for that namespace as well.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileUserOperatorRoleBindings() {
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

            return CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture)
                    .map((Void) null);
        } else {
            return roleBindingOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorRoleBinding(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the Topic Operator Config Map with logging configuration (Topic Operator does not have any metrics
     * configuration).
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileTopicOperagorConfigMap() {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            return Util.metricsAndLogging(reconciliation, configMapOperator, reconciliation.namespace(), entityOperator.topicOperator().getLogging(), null)
                    .compose(logging ->
                            configMapOperator.reconcile(
                                    reconciliation,
                                    reconciliation.namespace(),
                                    KafkaResources.entityTopicOperatorLoggingConfigMapName(reconciliation.name()),
                                    entityOperator.topicOperator().generateMetricsAndLogConfigMap(logging)
                            )
                    ).map((Void) null);
        } else {
            return configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorLoggingConfigMapName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the USer Operator Config Map with logging configuration (User Operator does not have any metrics
     * configuration).
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileUserOperatorConfigMap() {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            return Util.metricsAndLogging(reconciliation, configMapOperator, reconciliation.namespace(), entityOperator.userOperator().getLogging(), null)
                    .compose(logging ->
                            configMapOperator.reconcile(
                                    reconciliation,
                                    reconciliation.namespace(),
                                    KafkaResources.entityUserOperatorLoggingConfigMapName(reconciliation.name()),
                                    entityOperator.userOperator().generateMetricsAndLogConfigMap(logging)
                            )
                    ).map((Void) null);
        } else {
            return configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorLoggingConfigMapName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Deletes the Entity Operator secret. This Secret was used in the past when both Topic and User operators were
     * using the same user account. It is not used anymore, but we delete it if it exists from previous version.
     *
     * @return  Future which completes when the reconciliation is done
     */
    @SuppressWarnings("deprecation")
    Future<Void> deleteOldEntityOperatorSecret() {
        return secretOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorSecretName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Manages the Topic Operator certificates Secret.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileTopicOperatorSecret(Supplier<Date> dateSupplier) {
        if (entityOperator != null && entityOperator.topicOperator() != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()),
                                        entityOperator.topicOperator().generateSecret(clusterCa, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier)))
                                .compose(patchResult -> {
                                    if (patchResult instanceof ReconcileResult.Patched) {
                                        // The secret is patched and some changes to the existing certificates actually occurred
                                        existingEntityTopicOperatorCertsChanged = ModelUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
                                    } else {
                                        existingEntityTopicOperatorCertsChanged = false;
                                    }

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityTopicOperatorSecretName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the User Operator certificates Secret.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileUserOperatorSecret(Supplier<Date> dateSupplier) {
        if (entityOperator != null && entityOperator.userOperator() != null) {
            return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()),
                                        entityOperator.userOperator().generateSecret(clusterCa, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier)))
                                .compose(patchResult -> {
                                    if (patchResult instanceof ReconcileResult.Patched) {
                                        // The secret is patched and some changes to the existing certificates actually occurred
                                        existingEntityUserOperatorCertsChanged = ModelUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
                                    } else {
                                        existingEntityUserOperatorCertsChanged = false;
                                    }

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityUserOperatorSecretName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the Entity Operator Deployment.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> reconcileDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (entityOperator != null) {
            Deployment deployment = entityOperator.generateDeployment(isOpenShift, imagePullPolicy, imagePullSecrets);
            int caCertGeneration = ModelUtils.caCertGeneration(clusterCa);
            Annotations.annotations(deployment.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), deployment)
                    .compose(patchResult -> {
                        if (patchResult instanceof ReconcileResult.Noop)   {
                            // Deployment needs ot be rolled because the certificate secret changed
                            if (existingEntityTopicOperatorCertsChanged || existingEntityUserOperatorCertsChanged) {
                                return rollDeployment();
                            }
                        }

                        // No need to roll, we patched the deployment (and it will roll itself) or we created a new one
                        return Future.succeededFuture();
                    });
        } else  {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Triggers the rolling update of the Entity Operator. This is used to trigger the roll when the certificates change.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> rollDeployment() {
        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), operationTimeoutMs);
    }

    /**
     * Waits for the Entity Operator deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> waitForDeploymentReadiness() {
        if (entityOperator != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), KafkaResources.entityOperatorDeploymentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
