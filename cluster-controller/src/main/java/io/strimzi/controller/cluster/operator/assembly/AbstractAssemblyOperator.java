/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.strimzi.controller.cluster.model.Labels;
import io.strimzi.controller.cluster.operator.resource.ConfigMapOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Abstract assembly creation, update, read, deletion, etc.</p>
 *
 * <p>An assembly is a collection of Kubernetes resources of various types
 * (e.g. Services, StatefulSets, Deployments etc) which operate together to provide some functionality.</p>
 *
 * <p>This class manages a per-assembly locking strategy so only one operation per assembly
 * can proceed at once.</p>
 * @param <R> The type of resource from which the cluster state can be recovered
 */
public abstract class AbstractAssemblyOperator<
        R extends HasMetadata> {

    private static final Logger log = LoggerFactory.getLogger(AbstractAssemblyOperator.class.getName());

    protected static final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    protected final boolean isOpenShift;
    protected final String assemblyType;
    protected final String assemblyDescription;
    protected final ConfigMapOperator configMapOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift True iff running on OpenShift
     * @param assemblyDescription A description of the assembly, for logging. This is a high level description and different from
     *                           the {@code assemblyType} passed to {@link #getLockName(String, String, String)}
     */
    protected AbstractAssemblyOperator(Vertx vertx, boolean isOpenShift, String assemblyType,
                                       String assemblyDescription,
                                       ConfigMapOperator configMapOperations) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.assemblyType = assemblyType;
        this.assemblyDescription = assemblyDescription;
        this.configMapOperations = configMapOperations;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code assemblyType}, {@code namespace} and
     * cluster {@code name}
     * @param assemblyType The type of cluster
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    protected final String getLockName(String assemblyType, String namespace, String name) {
        return "lock::" + namespace + "::" + assemblyType + "::" + name;
    }

    /**
     * Subclasses implement this method to create or update the cluster. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param assemblyCm The name of the cluster.
     * @param handler Completion handler
     */
    protected abstract void createOrUpdate(ConfigMap assemblyCm, Handler<AsyncResult<Void>> handler);

    /**
     * Subclasses implement this method to delete the cluster.
     * @param namespace The namespace containing the cluster.
     * @param assemblyName The assemblyName of the cluster.
     * @param handler Completion handler
     */
    protected abstract void delete(String namespace, String assemblyName, Handler<AsyncResult<Void>> handler);

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
     * The name of the given {@code resource}, as read from its {@code strimzi.io/cluster} label.
     * @param resource The resource
     */
    protected String nameFromLabels(R resource) {
        return Labels.cluster(resource);
    }

    /**
     * Reconcile assembly resources in the given namespace having the given {@code assemblyName}.
     * Reconciliation works by getting the assembly ConfigMap in the given namespace with the given assemblyName and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(ConfigMap, Handler) created or updated} if ConfigMap is without same-named resources</li>
     * <li>An assembly will be {@linkplain #delete(String, String, Handler) deleted} if resources without same-named ConfigMap</li>
     * </ul>
     * @param namespace The namespace
     * @param assemblyName The name of the assembly
     */
    public final void reconcileAssembly(String namespace, String assemblyName) {
        final String lockName = getLockName(assemblyType, namespace, assemblyName);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                log.debug("Lock {} acquired", lockName);
                Lock lock = res.result();

                try {
                    log.info("Reconciling {} clusters ...", assemblyDescription);

                    // get ConfigMap and related resources for the specific cluster
                    ConfigMap cm = configMapOperations.get(namespace, assemblyName);

                    List<R> resources = getResources(namespace, Labels.forCluster(assemblyName));

                    if (cm != null) {
                        log.info("Reconciliation: {} assembly {} should be created or updated", assemblyDescription, assemblyName);
                        log.info("Creating/updating {} assembly {}", assemblyDescription, assemblyName);
                        createOrUpdate(cm, createResult -> {
                            if (createResult.succeeded()) {
                                log.info("{} assembly created/updated {}", assemblyDescription, assemblyName);
                            } else {
                                log.error("Failed to create/update {} assembly {}.", assemblyDescription, assemblyName);
                            }
                            lock.release();
                            log.debug("Lock {} released", lockName);
                        });
                    } else {

                        List<Future> result = new ArrayList<>(resources.size());
                        for (R resource : resources) {
                            log.info("Reconciliation: {} assembly {} should be deleted", assemblyDescription, resource.getMetadata().getName());
                            String nameFromResource = nameFromLabels(resource);
                            log.info("Deleting {} assembly {} in namespace {}", assemblyDescription, nameFromResource, namespace);

                            Future<Void> deleteFuture = Future.future();
                            result.add(deleteFuture);
                            delete(namespace, nameFromResource, deleteResult -> {
                                if (deleteResult.succeeded()) {
                                    log.info("{} assembly deleted {} in namespace {}", assemblyDescription, nameFromResource, namespace);
                                } else {
                                    log.error("Failed to delete {} assembly {} in namespace {}", assemblyDescription, nameFromResource, namespace);
                                }
                                deleteFuture.complete();
                            });
                        }

                        CompositeFuture.join(result).setHandler(res2 -> {
                            lock.release();
                            log.debug("Lock {} released", lockName);
                        });
                    }
                } catch (Throwable ex) {
                    log.error("Error while reconciling {} assembly", assemblyDescription, ex);
                    lock.release();
                    log.debug("Lock {} released", lockName);
                }
            } else {
                log.warn("Failed to acquire lock for {} assembly {}.", assemblyType, lockName);
            }
        });
    }

    /**
     * Reconcile assembly resources in the given namespace having the given selector.
     * Reconciliation works by getting the assembly ConfigMaps in the given namespace with the given selector and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(ConfigMap, Handler) created} for all ConfigMaps without same-named resources</li>
     * <li>An assembly will be {@linkplain #delete(String, String, Handler) deleted} for all resources without same-named ConfigMaps</li>
     * </ul>
     * @param namespace The namespace
     * @param selector The selector
     */
    public final void reconcileAll(String namespace, Labels selector) {
        Labels selectorWithCluster = selector.withType(assemblyType);

        // get ConfigMap for the corresponding cluster type
        List<ConfigMap> cms = configMapOperations.list(namespace, selectorWithCluster);
        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());

        // get resources for the corresponding cluster name (they are part of)
        List<R> resources = getResources(namespace, selectorWithCluster);
        Set<String> resourceNames = resources.stream().map(Labels::cluster).collect(Collectors.toSet());

        cmsNames.addAll(resourceNames);

        for (String name: cmsNames) {
            reconcileAssembly(namespace, name);
        }
    }

    /**
     * Gets the resources in the given namespace and with the given labels
     * from which an AbstractModel representing the current state of the assembly can be obtained.
     * @param namespace The namespace
     * @param selector The labels
     * @return The matching resources.
     */
    protected abstract List<R> getResources(String namespace, Labels selector);

}
