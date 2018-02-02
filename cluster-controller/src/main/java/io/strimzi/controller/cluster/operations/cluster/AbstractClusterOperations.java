/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.resources.AbstractCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    private final String clusterType;
    private final String operationType;
    protected final boolean isOpenShift;

    protected AbstractClusterOperations(Vertx vertx, boolean isOpenShift, String clusterType, String operationType) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.clusterType = clusterType;
        this.operationType = operationType;
    }

    protected final String getLockName(String namespace, String name) {
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

    private final void execute(String namespace, String name, CompositeOperation<C> compositeOperation, Handler<AsyncResult<Void>> handler) {
        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterOperation<C> clusterOp;
                try {
                    clusterOp = compositeOperation.getCluster(namespace, name);
                    log.info("{} {} cluster {} in namespace {}", operationType, clusterType, clusterOp.cluster().getName(), namespace);
                } catch (Exception ex) {
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

    protected abstract CompositeOperation<C> createOp();

    public final void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, createOp(), handler);
    }

    protected abstract CompositeOperation<C> deleteOp();

    public final void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, deleteOp(), handler);
    }

    protected abstract CompositeOperation<C> updateOp();

    public final void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, updateOp(), handler);
    }

}
