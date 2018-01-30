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

package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.resources.AbstractCluster;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract cluster creation, for a generic cluster type {@code C}.
 * This class applies the template method pattern, first obtaining the desired cluster configuration
 * ({@link Op#getCluster(K8SUtils, String, String)}),
 * then creating resources to match ({@link Op#futures(K8SUtils, String, AbstractCluster)}).
 *
 * This class manages a per-cluster-type and per-cluster locking strategy so only one operation per cluster
 * can proceed at once.
 * @param <C>
 */
public abstract class ClusterOperation<C extends AbstractCluster> {

    private static final Logger log = LoggerFactory.getLogger(ClusterOperation.class.getName());
    protected final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    private final String clusterType;
    private final String operationType;
    protected final KubernetesClient client;

    protected ClusterOperation(Vertx vertx, KubernetesClient client, String clusterType, String operationType) {
        this.vertx = vertx;
        this.client = client;
        this.clusterType = clusterType;
        this.operationType = operationType;
    }

    protected final String getLockName(String namespace, String name) {
        return "lock::"+ clusterType +"::" + namespace + "::" + name;
    }

    protected interface Op<C extends AbstractCluster> {
        /** Create the resources in Kubernetes according to the given {@code cluster} */
        List<Future> futures(KubernetesClient client, String namespace, C cluster);

        /** Get the desired Cluster instance */
        C getCluster(KubernetesClient client, String namespace, String name);
    }

    private final void execute(String namespace, String name, Op<C> op, Handler<AsyncResult<Void>> handler) {
        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                C cluster;
                try {
                    cluster = op.getCluster(client, namespace, name);
                    log.info("{} {} cluster {} in namespace {}", operationType, clusterType, cluster.getName(), namespace);
                } catch (Exception ex) {
                    log.error("Error while getting required {} cluster state for {} operation", clusterType, operationType, ex);
                    handler.handle(Future.failedFuture("getCluster error"));
                    lock.release();
                    return;
                }
                List<Future> list = op.futures(client, namespace, cluster);

                CompositeFuture.join(list).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("{} cluster {} in namespace {}: successful {}", clusterType, cluster.getName(), namespace, operationType);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("{} cluster {} in namespace {}: failed to {}", clusterType, cluster.getName(), namespace, operationType);
                        handler.handle(Future.failedFuture("Failed to create Kafka cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to {} {} cluster {}", operationType, clusterType, lockName);
                handler.handle(Future.failedFuture("Failed to acquire lock to " + operationType + " "+ clusterType + " cluster"));
            }
        });
    }

    protected abstract Op<C> createOp();

    public final void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, createOp(), handler);
    }


    protected abstract Op<C> deleteOp();

    public final void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, deleteOp(), handler);
    }

    public abstract void update(String namespace, String name, Handler<AsyncResult<Void>> handler);

}
