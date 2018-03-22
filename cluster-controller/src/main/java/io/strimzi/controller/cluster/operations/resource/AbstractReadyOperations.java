/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReadyOperations<C, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>, P>
        extends AbstractOperations<C, T, L, D, R, P> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Constructor.
     *
     * @param vertx        The vertx instance.
     * @param client       The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractReadyOperations(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    /**
     * Returns a future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     */
    public Future<Void> readiness(String namespace, String name, long pollIntervalMs, long timeoutMs) {
        Future<Void> fut = Future.future();
        log.info("Waiting for {} resource {} in namespace {} to get ready", resourceKind, name, namespace);
        long deadline = System.currentTimeMillis() + timeoutMs;

        try {
            while (true) {
                if (isReady(namespace, name)) {
                    fut.complete();
                    break;
                }
                Thread.sleep(1_000);
            }
        } catch (Throwable t) {
            fut.fail(t);
        }
        return fut;
/*

        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long timerId) {

                vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                    future -> {
                        try {
                            if (isReady(namespace, name))   {
                                future.complete();
                            } else {
                                if (log.isTraceEnabled()) {
                                    log.trace("{} {} in namespace {} is not ready", resourceKind, name, namespace);
                                }
                                future.fail("Not ready yet");
                            }
                        } catch (Throwable e) {
                            log.warn("Caught exception while waiting for {} {} in namespace {} to get ready", resourceKind, name, namespace, e);
                            future.fail(e);
                        }
                    },
                    true,
                    res -> {
                        if (res.succeeded()) {
                            log.info("{} {} in namespace {} is ready", resourceKind, name, namespace);
                            fut.complete();
                        } else {
                            long timeLeft = deadline - System.currentTimeMillis();
                            if (timeLeft <= 0) {
                                log.error("Exceeded timeoutMs of {} ms while waiting for {} {} in namespace {} to be ready", timeoutMs, resourceKind, name, namespace);
                                fut.fail(new TimeoutException());
                            } else {
                                // Schedule ourselves to run again
                                vertx.setTimer(Math.min(pollIntervalMs, timeLeft), this);
                            }
                        }
                    }
                );
            }
        };

        // Call the handler ourselves the first time
        handler.handle(null);

        return fut;*/
    }

    /**
     * Check if a resource is in the Ready state.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     */
    public boolean isReady(String namespace, String name) {
        R resourceOp = operation().inNamespace(namespace).withName(name);
        T resource = resourceOp.get();
        if (resource != null)   {
            if (Readiness.isReadinessApplicable(resource)) {
                return Boolean.TRUE.equals(resourceOp.isReady());
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
}
