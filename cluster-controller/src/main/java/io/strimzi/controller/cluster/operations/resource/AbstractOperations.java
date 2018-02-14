/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <D> The doneable variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractOperations<C, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {

    private static final Logger log = LoggerFactory.getLogger(AbstractOperations.class);
    protected final Vertx vertx;
    protected final C client;
    private final String resourceKind;

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractOperations(Vertx vertx, C client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract MixedOperation<T, L, D, R> operation();

    /**
     * Asynchronously create the given {@code resource} if it doesn't already exists,
     * returning a future for the outcome.
     * If the resource with that name already exists the future completes successfully.
     * @param resource The resource to create.
     */
    public Future<Void> create(T resource) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    String namespace = resource.getMetadata().getNamespace();
                    String name = resource.getMetadata().getName();
                    if (operation().inNamespace(namespace).withName(name).get() == null) {
                        try {
                            log.info("Creating {} {} in namespace {}", resourceKind, name, namespace);
                            operation().createOrReplace(resource);
                            log.info("{} {} in namespace {} has been created", resourceKind, name, namespace);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating {} {} in namespace {}", resourceKind, name, namespace, e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("{} {} in namespace {} already exists", resourceKind, name, namespace);
                        future.complete();
                    }
                },
                false,
                fut.completer()
        );
        return fut;
    }

    /**
     * Asynchronously delete the resource with the given {@code name} in the given {@code namespace},
     * returning a future for the outcome.
     * If the resource didn't exist the future completes successfully.
     * @param namespace The namespace of the resource to delete.
     * @param name The name of the resource to delete.
     */
    public Future<Void> delete(String namespace, String name) {
        Future fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (operation().inNamespace(namespace).withName(name).get() != null) {
                        try {
                            log.info("Deleting {} {} in namespace {}", resourceKind, name, namespace);
                            operation().inNamespace(namespace).withName(name).delete();
                            log.info("{} {} in namespace {} has been deleted", resourceKind, name, namespace);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while deleting {} {} in namespace {}", resourceKind, name, namespace, e);
                            future.fail(e);
                        }
                    } else {
                        log.warn("{} {} in namespace {} doesn't exist, so cannot be deleted", resourceKind, name, namespace);
                        future.complete();
                    }
                }, false,
                fut.completer()
        );
        return fut;
    }

    /**
     * Asynchronously patch the resource with the given {@code name} in the given {@code namespace}
     * with reflect the state given in the {@code patch}, returning a future for the outcome.
     * @param namespace The namespace of the resource to patch.
     * @param name The name of the resource to patch.
     * @param patch The desired state of the resource..
     */
    public Future<Void> patch(String namespace, String name, T patch) {
        return patch(namespace, name, true, patch);
    }

    public Future<Void> patch(String namespace, String name, boolean cascading, T patch) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Patching {} resource {} in namespace {} with {}", resourceKind, name, namespace, patch);
                        operation().inNamespace(namespace).withName(name).cascading(cascading).patch(patch);
                        log.info("{} {} in namespace {} has been patched", resourceKind, name, namespace);
                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
                        future.fail(e);
                    }
                },
                true,
                fut.completer()
        );
        return fut;
    }

    /**
     * Synchronously gets the resource with the given {@code name} in the given {@code namespace}.
     * @param namespace The namespace.
     * @param name The name.
     * @return The resource, or null if it doesn't exist.
     */
    public T get(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name).get();
    }

    /**
     * Synchronously list the resources in the given {@code namespace} with the given {@code labels}.
     * @param namespace The namespace.
     * @param labels The labels.
     * @return A list of matching resources.
     */
    public List<T> list(String namespace, Map<String, String> labels) {
        return operation().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    /**
     * Waits until resource is in the Ready state
     *
     * @param namespace The namespace
     * @param name The resource name
     * @param timeout Timeout in {@code timeUnit}
     * @param timeUnit Time unit
     */
    public Future<Void> waitUntilReady(String namespace, String name, long timeout, TimeUnit timeUnit) {
        Future<Void> fut = Future.future();
        log.info("Waiting for {} resource {} in namespace {} to get ready", resourceKind, name, namespace);
        AtomicInteger calls = new AtomicInteger(0);
        long timer = 1000L;
        long timeoutInMs = timeUnit.toMillis(timeout);

        vertx.setPeriodic(timer, timerId -> {
            calls.addAndGet(1);

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                    future -> {
                        try {
                            R resourceOp = operation().inNamespace(namespace).withName(name);
                            T resource = resourceOp.get();
                            if (resource != null)   {
                                if (Readiness.isReadinessApplicable(resource)) {
                                    Boolean ready = resourceOp.isReady();

                                    if (ready == true) {
                                        future.complete();
                                    } else {
                                        future.fail("Not ready yet");
                                    }
                                } else {
                                    future.complete();
                                }
                            } else {
                                log.warn("{} {} in namespace {} doesn't exist", resourceKind, name, namespace);
                                future.fail("Resource doesn't exist");
                            }
                        }
                        catch (Exception e) {
                            log.warn("Caught exception while waiting for {} {} in namespace {} to get ready", resourceKind, name, namespace, e);
                            future.fail(e);
                        }
                    },
                    false,
                    res -> {
                        if (res.succeeded())    {
                            vertx.cancelTimer(timerId);
                            log.info("{} {} in namespace {} is ready", resourceKind, name, namespace);
                            fut.complete();
                        } else {
                            if (calls.get() * timer > timeoutInMs)   {
                                vertx.cancelTimer(timerId);
                                log.error("Exceeded timeout of {} ms while waiting for {} {} in namespace {} to be ready", timeoutInMs, resourceKind, name, namespace);
                                fut.fail("Timeout while waiting for resource to be ready");
                            }
                        }
                    }
            );
        });

        return fut;
    }
}
