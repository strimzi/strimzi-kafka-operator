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

import io.strimzi.controller.cluster.K8SUtils;
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
 * ({@link #getCluster(K8SUtils, Handler, Lock)}),
 * then creating resources to match ({@link #futures(K8SUtils, AbstractCluster)}).
 *
 * This class manages a per-cluster-type and per-cluster locking strategy so only one operation per cluster
 * can proceed at once.
 * @param <C>
 */
public abstract class SimpleClusterOperation<C extends AbstractCluster> extends ClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateKafkaClusterOperation.class.getName());
    private final String clusterType;
    private final String operationType;
    private final K8SUtils k8s;

    protected SimpleClusterOperation(Vertx vertx, K8SUtils k8s, String clusterType, String operationType, String namespace, String name) {
        super(vertx, namespace, name);
        this.k8s = k8s;
        this.clusterType = clusterType;
        this.operationType = operationType;
    }

    @Override
    protected final String getLockName() {
        return "lock::"+ clusterType +"::" + namespace + "::" + name;
    }

    public final void execute(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                C cluster;
                try {
                    cluster = getCluster(k8s, handler, lock);
                    log.info("{} {} cluster {} in namespace {}", operationType, clusterType, cluster.getName(), namespace);
                } catch (Exception ex) {
                    log.error("Error while getting required {} cluster state for {} operation", clusterType, operationType, ex);
                    handler.handle(Future.failedFuture("getCluster error"));
                    lock.release();
                    return;
                }
                List<Future> list = futures(k8s, cluster);

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
                log.error("Failed to acquire lock to {} {} cluster {}", operationType, clusterType, getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to " + operationType + " "+ clusterType + " cluster"));
            }
        });
    }

    /** Create the resources in Kubernetes according to the given {@code cluster} */
    protected abstract List<Future> futures(K8SUtils k8s, C cluster);

    /** Get the desired Cluster instance */
    protected abstract C getCluster(K8SUtils k8s, Handler<AsyncResult<Void>> handler, Lock lock);

}
