/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.resources.AbstractCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Abstract cluster creation, update, read, delection, etc, for a generic cluster type {@code C}.
 * This class applies the template method pattern, first obtaining the desired cluster configuration
 * ({@link CompositeOperation#getCluster(String, String)}),
 * then creating resources to match ({@link CompositeOperation#composite(String, ClusterOperation)}.
 *
 * This class manages a per-cluster-type and per-cluster locking strategy so only one operation per cluster
 * can proceed at once.
 * @param <C> The type of Kubernetes client
 */
public abstract class AbstractClusterOperations<C extends AbstractCluster> {

    private static final Logger log = LoggerFactory.getLogger(AbstractClusterOperations.class.getName());

    protected static final String OP_CREATE = "create";
    protected static final String OP_DELETE = "delete";
    protected static final String OP_UPDATE = "update";

    protected final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    protected final boolean isOpenShift;
    protected final String clusterDescription;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift True iff running on OpenShift
     * @param clusterDescription A description of the cluster, for logging. This is a high level description and different from
     *                           the {@code clusterType} passed to {@link #getLockName(String, String, String)}
     *                           and {@link #execute(String, String, String, String, CompositeOperation, Handler)}
     */
    protected AbstractClusterOperations(Vertx vertx, boolean isOpenShift, String clusterDescription) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.clusterDescription = clusterDescription;
    }

    protected final String getLockName(String clusterType, String namespace, String name) {
        return "lock::"+ clusterType +"::" + namespace + "::" + name;
    }

    protected static class ClusterOperation<C extends AbstractCluster> {
        private final C cluster;
        private final ClusterDiffResult diff;

        public ClusterOperation(C cluster, ClusterDiffResult diff) {
            this.cluster = cluster;
            this.diff = diff;
        }

        public C cluster() {
            return cluster;
        }

        public ClusterDiffResult diff() {
            return diff;
        }

    }

    protected interface CompositeOperation<C extends AbstractCluster> {
        /**
         * Create the resources in Kubernetes according to the given {@code cluster},
         * returning a composite future for when the overall operation is done
         */
        Future<?> composite(String namespace, ClusterOperation<C> operation);

        /** Get the desired Cluster instance */
        ClusterOperation<C> getCluster(String namespace, String name);
    }

    protected final <C extends AbstractCluster> void execute(String clusterType, String operationType, String namespace, String name, CompositeOperation<C> compositeOperation, Handler<AsyncResult<Void>> handler) {
        final String lockName = getLockName(clusterType, namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterOperation<C> clusterOp;
                try {
                    clusterOp = compositeOperation.getCluster(namespace, name);
                    log.info("{} {} cluster {} in namespace {}", operationType, clusterType, clusterOp.cluster().getName(), namespace);
                } catch (Throwable ex) {
                    log.error("Error while getting required {} cluster state for {} operation", clusterType, operationType, ex);
                    handler.handle(Future.failedFuture("getCluster error"));
                    lock.release();
                    return;
                }
                Future<?> composite = compositeOperation.composite(namespace, clusterOp);

                composite.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("{} cluster {} in namespace {}: successful {}", clusterType, clusterOp.cluster().getName(), namespace, operationType);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("{} cluster {} in namespace {}: failed to {}", clusterType, clusterOp.cluster().getName(), namespace, operationType);
                        handler.handle(Future.failedFuture("Failed to execute cluster operation"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to {} {} cluster {}", operationType, clusterType, lockName);
                handler.handle(Future.failedFuture("Failed to acquire lock to " + operationType + " "+ clusterType + " cluster"));
            }
        });
    }

    protected abstract void create(String namespace, String name, Handler<AsyncResult<Void>> handler);

    public void create(String namespace, String name)   {
        log.info("Adding {} cluster {}", clusterDescription, name);

        create(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("{} cluster added {}", clusterDescription, name);
            }
            else {
                log.error("Failed to add {} cluster {}.", clusterDescription, name);
            }
        });
    }

    protected abstract void delete(String namespace, String name, Handler<AsyncResult<Void>> handler);

    public void delete(String namespace, String name)   {
        log.info("Deleting {} cluster {} in namespace {}", clusterDescription, name, namespace);
        delete(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("{} cluster deleted {} in namespace {}", clusterDescription, name, namespace);
            }
            else {
                log.error("Failed to delete {} cluster {} in namespace {}", clusterDescription, name, namespace);
            }
        });
    }

    protected abstract void update(String namespace, String name, Handler<AsyncResult<Void>> handler);

    public void update(String namespace, String name)   {

        log.info("Checking for updates in {} cluster {}", clusterDescription, name);
        update(namespace, name, res2 -> {
            if (res2.succeeded()) {
                log.info("{} cluster updated {}", clusterDescription, name);
            }
            else {
                log.error("Failed to update {} cluster {}.", clusterDescription, name);
            }
        });
    }

    protected String name(HasMetadata resource) {
        return resource.getMetadata().getName();
    }

    protected String nameFromLabels(HasMetadata resource) {
        return resource.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL);
    }

    public abstract void reconcile(String namespace, Map<String, String> labels);

}
