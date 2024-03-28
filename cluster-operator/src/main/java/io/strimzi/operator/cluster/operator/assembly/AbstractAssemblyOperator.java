/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractWatchableStatusedNamespacedResourceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;

/**
 * <p>Abstract assembly creation, update, read, deletion, etc.</p>
 *
 * <p>An assembly is a collection of Kubernetes resources of various types
 * (e.g. Services, StatefulSets, Deployments etc) which operate together to provide some functionality.</p>
 *
 * <p>This class manages a per-assembly locking strategy so only one operation per assembly
 * can proceed at once.</p>
 */
public abstract class AbstractAssemblyOperator<C extends KubernetesClient, T extends CustomResource<P, S>,
        L extends KubernetesResourceList<T>, R extends Resource<T>, P extends Spec, S extends Status>
    extends AbstractOperator<T, P, S, AbstractWatchableStatusedNamespacedResourceOperator<C, T, L, R>> {
    protected final PlatformFeaturesAvailability pfa;
    protected final SecretOperator secretOperations;
    protected final CertManager certManager;
    protected final PasswordGenerator passwordGenerator;
    protected final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    protected final ServiceOperator serviceOperations;
    protected final ConfigMapOperator configMapOperations;
    protected final ClusterRoleBindingOperator clusterRoleBindingOperations;
    protected final ServiceAccountOperator serviceAccountOperations;
    protected final ImagePullPolicy imagePullPolicy;
    protected final List<LocalObjectReference> imagePullSecrets;
    protected final KafkaVersion.Lookup versions;
    protected long operationTimeoutMs;

    /**
     * @param vertx The Vertx instance
     * @param pfa Properties with features availability
     * @param kind The kind of watched resource
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param resourceOperator For operating on the desired resource
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    protected AbstractAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, String kind,
                                       CertManager certManager, PasswordGenerator passwordGenerator,
                                       AbstractWatchableStatusedNamespacedResourceOperator<C, T, L, R> resourceOperator,
                                       ResourceOperatorSupplier supplier,
                                       ClusterOperatorConfig config) {
        super(vertx, kind, resourceOperator, supplier.metricsProvider, config.getCustomResourceSelector());
        this.pfa = pfa;
        this.certManager = certManager;
        this.passwordGenerator = passwordGenerator;
        this.secretOperations = supplier.secretOperations;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.configMapOperations = supplier.configMapOperations;
        this.serviceOperations = supplier.serviceOperations;
        this.clusterRoleBindingOperations = supplier.clusterRoleBindingOperator;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.versions = config.versions();
        this.operationTimeoutMs = config.getOperationTimeoutMs();
    }

    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return Future.succeededFuture(Boolean.FALSE);
    }
}
