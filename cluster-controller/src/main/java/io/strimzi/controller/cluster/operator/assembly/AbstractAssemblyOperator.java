/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.strimzi.controller.cluster.Reconciliation;
import io.strimzi.controller.cluster.model.AssemblyType;
import io.strimzi.controller.cluster.model.Labels;
import io.strimzi.controller.cluster.operator.resource.ConfigMapOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * <p>Abstract assembly creation, update, read, deletion, etc.</p>
 *
 * <p>An assembly is a collection of Kubernetes resources of various types
 * (e.g. Services, StatefulSets, Deployments etc) which operate together to provide some functionality.</p>
 *
 * <p>This class manages a per-assembly locking strategy so only one operation per assembly
 * can proceed at once.</p>
 */
public abstract class AbstractAssemblyOperator {

    private static final Logger log = LoggerFactory.getLogger(AbstractAssemblyOperator.class.getName());

    protected static final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    protected final boolean isOpenShift;
    protected final AssemblyType assemblyType;
    protected final ConfigMapOperator configMapOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift True iff running on OpenShift
     */
    protected AbstractAssemblyOperator(Vertx vertx, boolean isOpenShift, AssemblyType assemblyType,
                                       ConfigMapOperator configMapOperations) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.assemblyType = assemblyType;
        this.configMapOperations = configMapOperations;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code assemblyType}, {@code namespace} and
     * cluster {@code name}
     * @param assemblyType The type of cluster
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    protected final String getLockName(AssemblyType assemblyType, String namespace, String name) {
        return "lock::" + namespace + "::" + assemblyType + "::" + name;
    }

    /**
     * Subclasses implement this method to create or update the cluster. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param assemblyCm The name of the cluster.
     * @param handler Completion handler
     */
    protected abstract void createOrUpdate(Reconciliation reconciliation, ConfigMap assemblyCm, Handler<AsyncResult<Void>> handler);

    /**
     * Subclasses implement this method to delete the cluster.
     * @param handler Completion handler
     */
    protected abstract void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler);

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
     * Reconcile assembly resources in the given namespace having the given {@code assemblyName}.
     * Reconciliation works by getting the assembly ConfigMap in the given namespace with the given assemblyName and
     * comparing with the corresponding {@linkplain #getResources(String) resource}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(Reconciliation, ConfigMap, Handler) created or updated} if ConfigMap is without same-named resources</li>
     * <li>An assembly will be {@linkplain #delete(Reconciliation, Handler) deleted} if resources without same-named ConfigMap</li>
     * </ul>
     */
    public final void reconcileAssembly(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String assemblyName = reconciliation.assemblyName();
        final String lockName = getLockName(assemblyType, namespace, assemblyName);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                log.debug("{}: Lock {} acquired", reconciliation, lockName);
                Lock lock = res.result();

                try {
                    // get ConfigMap and related resources for the specific cluster
                    ConfigMap cm = configMapOperations.get(namespace, assemblyName);

                    if (cm != null) {
                        log.info("{}: assembly {} should be created or updated", reconciliation, assemblyName);
                        createOrUpdate(reconciliation, cm, createResult -> {
                            lock.release();
                            log.debug("{}: Lock {} released", reconciliation, lockName);
                            handler.handle(createResult);
                        });
                    } else {
                        log.info("{}: assembly {} should be deleted", reconciliation, assemblyName);
                        delete(reconciliation, deleteResult -> {
                            lock.release();
                            log.debug("{}: Lock {} released", reconciliation, lockName);
                            handler.handle(deleteResult);
                        });
                    }
                } catch (Throwable ex) {
                    lock.release();
                    log.debug("{}: Lock {} released", reconciliation, lockName);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.warn("{}: Failed to acquire lock {}.", reconciliation, lockName);
            }
        });
    }

    /**
     * Reconcile assembly resources in the given namespace having the given selector.
     * Reconciliation works by getting the assembly ConfigMaps in the given namespace with the given selector and
     * comparing with the corresponding {@linkplain #getResources(String) resource}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(Reconciliation, ConfigMap, Handler) created} for all ConfigMaps without same-named resources</li>
     * <li>An assembly will be {@linkplain #delete(Reconciliation, Handler) deleted} for all resources without same-named ConfigMaps</li>
     * </ul>
     *
     * @param trigger A description of the triggering event (timer or watch), used for logging
     * @param namespace The namespace
     * @param selector The selector
     */
    public final CountDownLatch reconcileAll(String trigger, String namespace, Labels selector) {
        Labels selectorWithCluster = selector.withType(assemblyType);

        // get ConfigMaps with kind=cluster&type=kafka (or connect, or connect-s2i) for the corresponding cluster type
        List<ConfigMap> cms = configMapOperations.list(namespace, selectorWithCluster);
        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): ConfigMaps with labels {}: {}", assemblyType, trigger, selectorWithCluster, cmsNames);

        // get resources with kind=cluster&type=kafka (or connect, or connect-s2i)
        List<? extends HasMetadata> resources = getResources(namespace);
        // now extract the cluster name from those
        Set<String> resourceNames = resources.stream()
                .filter(r -> Labels.kind(r) == null) // exclude Cluster CM, which won't have a cluster label
                .map(Labels::cluster)
                .collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): Other resources with labels {}: {}", assemblyType, trigger, selectorWithCluster, resourceNames);

        cmsNames.addAll(resourceNames);

        // We use a latch so that callers (specifically, test callers) know when the reconciliation is complete
        // Using futures would be more complex for no benefit
        CountDownLatch latch = new CountDownLatch(cmsNames.size());

        for (String name: cmsNames) {
            Reconciliation reconciliation = new Reconciliation(trigger, assemblyType, namespace, name);
            reconcileAssembly(reconciliation, result -> {
                if (result.succeeded()) {
                    log.info("{}: Assembly reconciled", reconciliation);
                } else {
                    log.error("{}: Failed to reconcile", reconciliation, result.cause());
                }
                latch.countDown();
            });
        }

        return latch;
    }

    /**
     * Gets all the assembly resources (for all assemblies) in the given namespace.
     * Assembly CMs may be included in the result.
     * @param namespace The namespace
     * @return The matching resources.
     */
    protected abstract List<HasMetadata> getResources(String namespace);

}
