/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class AbstractOperator<C extends KubernetesClient,
        T extends CustomResource,
        L extends CustomResourceList<T>,
        D extends Doneable<T>> {

    private static final Logger log = LogManager.getLogger(AbstractOperator.class);
    private static final int LOCK_TIMEOUT_MS = 10;
    protected final Vertx vertx;
    protected final CrdOperator<C, T, L, D> crdOperator;
    private final String kind;
    private final ResourceType resourceType;

    public AbstractOperator(Vertx vertx, ResourceType resourceType, CrdOperator<C, T, L, D> crdOperator) {
        this.vertx = vertx;
        this.kind = resourceType.name;
        this.resourceType = resourceType;
        this.crdOperator = crdOperator;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code namespace} and
     * cluster {@code name}
     *
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    private final String getLockName(String namespace, String name) {
        return "lock::" + namespace + "::" + kind + "::" + name;
    }

    protected abstract Future<Void> createOrUpdate(Reconciliation reconciliation, T cr);

    protected abstract Future<Void> delete(Reconciliation reconciliation);

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaUser})
     * in the given namespace with the given name and
     * comparing with the corresponding resource.
     * @param reconciliation The reconciliation.
     * @return A Future which is completed with the result of the reconciliation.
     */
    public final Future<Void> reconcile(Reconciliation reconciliation) {
        Future<Void> handler = Future.future();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT_MS, res -> {
            if (res.succeeded()) {
                log.debug("{}: Lock {} acquired", reconciliation, lockName);
                Lock lock = res.result();

                try {
                    T cr = crdOperator.get(namespace, name);
                    if (cr != null) {
                        log.info("{}: {} {} should be created or updated", reconciliation, kind, name);

                        createOrUpdate(reconciliation, cr).setHandler(createResult -> {
                            lock.release();
                            log.debug("{}: Lock {} released", reconciliation, lockName);
                            if (createResult.failed()) {
                                log.error("{}: createOrUpdate failed", reconciliation, createResult.cause());
                            } else {
                                handler.handle(createResult);
                            }
                        });
                    } else {
                        log.info("{}: {} {} should be deleted", reconciliation, kind, name);
                        delete(reconciliation).setHandler(deleteResult -> {
                            if (deleteResult.succeeded())   {
                                log.info("{}: {} {} deleted", reconciliation, kind, name);
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                handler.handle(deleteResult);
                            } else {
                                log.error("{}: Deletion of {} {} failed", reconciliation, kind, name, deleteResult.cause());
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                handler.handle(deleteResult);
                            }
                        });
                    }
                } catch (Throwable ex) {
                    lock.release();
                    log.error("{}: Reconciliation failed", reconciliation, ex);
                    log.debug("{}: Lock {} released", reconciliation, lockName);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.warn("{}: Failed to acquire lock {}.", reconciliation, lockName);
            }
        });
        return handler;
    }

    /**
     * Reconcile User resources in the given namespace having the given selector.
     * Reconciliation works by getting the KafkaUSer custom resources in the given namespace with the given selector and
     * comparing with the corresponding resource.
     *
     * @param trigger A description of the triggering event (timer or watch), used for logging
     * @param namespace The namespace
     * @param selector The labels used to select the resources
     * @return A latch for awaiting the reconciliation.
     */
    public final CountDownLatch reconcileAll(String trigger, String namespace, Labels selector) {
        CountDownLatch outerLatch = new CountDownLatch(1);

        allResourceNames(namespace, selector).setHandler(ar -> {
            if (ar.succeeded()) {
                Set<String> desiredNames = ar.result();
                // We use a latch so that callers (specifically, test callers) know when the reconciliation is complete
                // Using futures would be more complex for no benefit
                AtomicInteger counter = new AtomicInteger(desiredNames.size());

                for (String name : desiredNames) {
                    Reconciliation reconciliation = new Reconciliation(trigger, ResourceType.USER, namespace, name);
                    reconcile(reconciliation).setHandler(result -> {
                        handleResult(reconciliation, result);
                        if (counter.getAndDecrement() == 0) {
                            outerLatch.countDown();
                        }
                    });
                }
            } else {
                outerLatch.countDown();
            }
        });
        return outerLatch;
    }

    protected Future<Set<String>> allResourceNames(String namespace, Labels selector) {
        return crdOperator.listAsync(namespace, selector).map(desiredResources ->
            desiredResources.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet()));
    }

    /**
     * Create Kubernetes watch for KafkaUser resources.
     *
     * @param namespace Namespace where to watch for users.
     * @param selector Labels which the Users should match.
     * @param onClose Callback called when the watch is closed.
     *
     * @return A future which completes when the watcher has been created.
     */
    public Future<Watch> createWatch(String namespace, Labels selector, Consumer<KubernetesClientException> onClose) {
        Future<Watch> result = Future.future();
        vertx.executeBlocking(
            future -> {
                Watch watch = crdOperator.watch(namespace, selector, new Watcher<T>() {
                    @Override
                    public void eventReceived(Action action, T crd) {
                        String name = crd.getMetadata().getName();
                        switch (action) {
                            case ADDED:
                            case DELETED:
                            case MODIFIED:
                                Reconciliation reconciliation = new Reconciliation("watch", resourceType, namespace, name);
                                log.info("{}: {} {} in namespace {} was {}", reconciliation, kind, name, namespace, action);
                                reconcile(reconciliation).setHandler(result -> {
                                    handleResult(reconciliation, result);
                                });
                                break;
                            case ERROR:
                                log.error("Failed {} {} in namespace{} ", kind, name, namespace);
                                reconcileAll("watch error", namespace, selector);
                                break;
                            default:
                                log.error("Unknown action: {} in namespace {}", name, namespace);
                                reconcileAll("watch unknown", namespace, selector);
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
            log.info("{}: User reconciled", reconciliation);
        } else {
            Throwable cause = result.cause();
            log.warn("{}: Failed to reconcile {}", reconciliation, cause);
        }
    }
}
