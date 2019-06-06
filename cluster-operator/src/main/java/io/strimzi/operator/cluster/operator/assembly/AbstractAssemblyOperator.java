/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.model.ResourceVisitor;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.InvalidConfigParameterException;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.model.ValidationVisitor;
import io.strimzi.operator.common.operator.resource.AbstractWatchableResourceOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

/**
 * <p>Abstract assembly creation, update, read, deletion, etc.</p>
 *
 * <p>An assembly is a collection of Kubernetes resources of various types
 * (e.g. Services, StatefulSets, Deployments etc) which operate together to provide some functionality.</p>
 *
 * <p>This class manages a per-assembly locking strategy so only one operation per assembly
 * can proceed at once.</p>
 */
public abstract class AbstractAssemblyOperator<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/, D extends Doneable<T>, R extends Resource<T, D>> {

    private static final Logger log = LogManager.getLogger(AbstractAssemblyOperator.class.getName());

    protected static final int LOCK_TIMEOUT_MS = 10000;

    protected final Vertx vertx;
    protected final PlatformFeaturesAvailability pfa;
    protected final ResourceType assemblyType;
    protected final AbstractWatchableResourceOperator<C, T, L, D, R> resourceOperator;
    protected final SecretOperator secretOperations;
    protected final CertManager certManager;
    protected final NetworkPolicyOperator networkPolicyOperator;
    protected final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    protected final ServiceOperator serviceOperations;
    protected final ConfigMapOperator configMapOperations;
    protected final ClusterRoleBindingOperator clusterRoleBindingOperations;
    protected final ServiceAccountOperator serviceAccountOperations;
    protected final ImagePullPolicy imagePullPolicy;
    protected final List<LocalObjectReference> imagePullSecrets;
    protected final KafkaVersion.Lookup versions;
    private final String kind;

    /**
     * @param vertx The Vertx instance
     * @param pfa Properties with features availability
     * @param assemblyType Assembly type
     * @param certManager Certificate manager
     * @param resourceOperator For operating on the desired resource
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    protected AbstractAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, ResourceType assemblyType,
                                       CertManager certManager,
                                       AbstractWatchableResourceOperator<C, T, L, D, R> resourceOperator,
                                       ResourceOperatorSupplier supplier,
                                       ClusterOperatorConfig config) {
        this.vertx = vertx;
        this.pfa = pfa;
        this.assemblyType = assemblyType;
        this.kind = assemblyType.name;
        this.resourceOperator = resourceOperator;
        this.certManager = certManager;
        this.secretOperations = supplier.secretOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.configMapOperations = supplier.configMapOperations;
        this.serviceOperations = supplier.serviceOperations;
        this.clusterRoleBindingOperations = supplier.clusterRoleBindingOperator;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.versions = config.versions();
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code assemblyType}, {@code namespace} and
     * cluster {@code name}
     * @param assemblyType The type of cluster
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    protected final String getLockName(ResourceType assemblyType, String namespace, String name) {
        return "lock::" + namespace + "::" + assemblyType + "::" + name;
    }

    /**
     * Subclasses implement this method to create or update the cluster. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param reconciliation Unique identification for the reconciliation
     * @param assemblyResource Resources with the desired cluster configuration.
     * @return A future which is completed when the resource has been reconciled.
     */
    protected abstract Future<Void> createOrUpdate(Reconciliation reconciliation, T assemblyResource);

    /**
     * The name of the given {@code resource}, as read from its metadata.
     * @param resource The resource
     */
    protected static String name(HasMetadata resource) {
        if (resource != null) {
            ObjectMeta metadata = resource.getMetadata();
            if (metadata != null) {
                return metadata.getName();
            }
        }
        return null;
    }

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaAssembly}) in the given namespace with the given name and
     * comparing with the corresponding resources}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(Reconciliation, HasMetadata) created or updated} if CustomResource is without same-named resources</li>
     * <li>An assembly will be deleted automatically by garbage collection when the custom resoruce is deleted</li>
     * </ul>
     * @param reconciliation The reconciliation.
     * @param handler The result handler.
     */
    public final void reconcileAssembly(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String assemblyName = reconciliation.name();
        final String lockName = getLockName(assemblyType, namespace, assemblyName);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT_MS, res -> {
            if (res.succeeded()) {
                log.debug("{}: Lock {} acquired", reconciliation, lockName);
                Lock lock = res.result();

                try {
                    // get CustomResource and related resources for the specific cluster
                    T cr = resourceOperator.get(namespace, assemblyName);
                    validate(cr);

                    if (cr != null) {
                        log.info("{}: Assembly {} should be created or updated", reconciliation, assemblyName);
                        createOrUpdate(reconciliation, cr)
                            .setHandler(createResult -> {
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                if (createResult.failed()) {
                                    if (createResult.cause() instanceof InvalidResourceException) {
                                        log.error("{}: createOrUpdate failed. {}", reconciliation, createResult.cause().getMessage());
                                    } else {
                                        log.error("{}: createOrUpdate failed", reconciliation, createResult.cause());
                                    }
                                } else {
                                    handler.handle(createResult);
                                }
                            });
                    } else {
                        log.info("{}: Assembly {} should be deleted by garbage collection", reconciliation, assemblyName);
                        lock.release();
                        log.debug("{}: Lock {} released", reconciliation, lockName);
                        handler.handle(Future.succeededFuture());
                    }
                } catch (Throwable ex) {
                    lock.release();
                    log.debug("{}: Lock {} released", reconciliation, lockName);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.debug("{}: Failed to acquire lock {}.", reconciliation, lockName);
            }
        });
    }

    /**
     * Validate the Custom Resource.
     * This should log at the WARN level (rather than throwing) if the resource can safely be reconciled.
     * @param resource The custom resource
     * @throws InvalidResourceException if the resource cannot be safely reconciled.
     */
    protected void validate(T resource) {
        if (resource != null) {
            ResourceVisitor.visit(resource, new ValidationVisitor(resource, log));
        }
    }


    /**
     * Reconcile assembly resources in the given namespace having the given selector.
     * Reconciliation works by getting the assembly ConfigMaps in the given namespace with the given selector and
     * comparing with the corresponding resources}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(Reconciliation, HasMetadata) created} for all Custom Resource without same-named resources</li>
     * <li>An assembly will be deleted automatically by the garbage collection when the Custom Resource is deleted</li>
     * </ul>
     *
     * @param trigger A description of the triggering event (timer or watch), used for logging
     * @param namespace The namespace
     * @return A latch for knowing when reconciliation is complete.
     */
    public final CountDownLatch reconcileAll(String trigger, String namespace) {

        // get Kafka CustomResources (or Connect, Connect-s2i, or Mirror Maker)
        List<T> desiredResources = resourceOperator.list(namespace, Labels.EMPTY);
        Set<NamespaceAndName> desiredNames = desiredResources.stream()
                .map(cr -> new NamespaceAndName(cr.getMetadata().getNamespace(), cr.getMetadata().getName()))
                .collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): desired resources with labels {}: {}", assemblyType, trigger, Labels.EMPTY, desiredNames);

        // We use a latch so that callers (specifically, test callers) know when the reconciliation is complete
        // Using futures would be more complex for no benefit
        CountDownLatch latch = new CountDownLatch(desiredNames.size());

        for (NamespaceAndName name: desiredNames) {
            Reconciliation reconciliation = new Reconciliation(trigger, assemblyType, name.getNamespace(), name.getName());
            reconcileAssembly(reconciliation, result -> {
                handleResult(reconciliation, result);
                latch.countDown();
            });
        }

        return latch;
    }

    /**
     * Creates a watch on resources in the given namespace.
     * @param watchNamespace The namespace to watch.
     * @param onClose A consumer for any exceptions causing the closing of the watcher.
     * @return A future which completes when watcher has been created.
     */
    public Future<Watch> createWatch(String watchNamespace, Consumer<KubernetesClientException> onClose) {
        Future<Watch> result = Future.future();
        vertx.<Watch>executeBlocking(
            future -> {
                Watch watch = resourceOperator.watch(watchNamespace, new Watcher<T>() {
                    @Override
                    public void eventReceived(Action action, T cr) {
                        String name = cr.getMetadata().getName();
                        String resourceNamespace = cr.getMetadata().getNamespace();
                        switch (action) {
                            case ADDED:
                            case DELETED:
                            case MODIFIED:
                                Reconciliation reconciliation = new Reconciliation("watch", assemblyType, resourceNamespace, name);
                                log.info("{}: {} {} in namespace {} was {}", reconciliation, kind, name, resourceNamespace, action);
                                reconcileAssembly(reconciliation, result -> {
                                    handleResult(reconciliation, result);
                                });
                                break;
                            case ERROR:
                                log.error("Failed {} {} in namespace{} ", kind, name, resourceNamespace);
                                reconcileAll("watch error", watchNamespace);
                                break;
                            default:
                                log.error("Unknown action: {} in namespace {}", name, resourceNamespace);
                                reconcileAll("watch unknown", watchNamespace);
                        }
                    }

                    @Override
                    public void onClose(KubernetesClientException e) {
                        onClose.accept(e);
                    }
                });
                future.complete(watch);
            }, result
        );
        return result;
    }

    /**
     * Log the reconciliation outcome.
     */
    private void handleResult(Reconciliation reconciliation, AsyncResult<Void> result) {
        if (result.succeeded()) {
            log.info("{}: Assembly reconciled", reconciliation);
        } else {
            Throwable cause = result.cause();
            if (cause instanceof InvalidConfigParameterException) {
                log.warn("{}: Failed to reconcile {}", reconciliation, cause.getMessage());
            } else {
                log.warn("{}: Failed to reconcile", reconciliation, cause);
            }
        }
    }

    /**
     * @param current Previsous ConfigMap
     * @param desired Desired ConfigMap
     * @return Returns true if only metrics settings has been changed
     */
    public boolean onlyMetricsSettingChanged(ConfigMap current, ConfigMap desired) {
        if ((current == null && desired != null) || (current != null && desired == null)) {
            // Metrics were added or deleted. We want rolling update
            return false;
        }
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(desired));
        boolean onlyMetricsSettingChanged = false;
        for (JsonNode d : diff) {
            if (d.get("path").asText().equals("/data/metrics-config.yml") && d.get("op").asText().equals("replace")) {
                onlyMetricsSettingChanged = true;
            }
        }
        return onlyMetricsSettingChanged && diff.size() == 1;
    }

}
