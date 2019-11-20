/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.ResourceVisitor;
import io.strimzi.operator.common.model.ValidationVisitor;
import io.strimzi.operator.common.operator.resource.AbstractWatchableResourceOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A base implementation of {@link Operator}.
 *
 * <ul>
 * <li>uses the Fabric8 kubernetes API and implements
 * {@link #reconcile(Reconciliation)} by delegating to abstract {@link #createOrUpdate(Reconciliation, HasMetadata)}
 * and {@link #delete(Reconciliation)} methods for subclasses to implement.
 * 
 * <li>add support for operator-side {@linkplain #validate(HasMetadata) validation}.
 *     This can be used to automatically log warnings about source resources which used deprecated part of the CR API.
 *
 * </ul>
 * @param <T> The Java representation of the Kubernetes resource, e.g. {@code Kafka} or {@code KafkaConnect}
 * @param <S> The "Resource Operator" for the source resource type. Typically this will be some instantiation of
 *           {@link io.strimzi.operator.common.operator.resource.CrdOperator}.
 */
public abstract class AbstractOperator<
        T extends HasMetadata,
        S extends AbstractWatchableResourceOperator<?, T, ?, ?, ?>>
            implements Operator {

    private static final Logger log = LogManager.getLogger(AbstractOperator.class);
    protected static final int LOCK_TIMEOUT_MS = 10000;
    protected final Vertx vertx;
    protected final S resourceOperator;
    private final String kind;

    public AbstractOperator(Vertx vertx, String kind, S resourceOperator) {
        this.vertx = vertx;
        this.kind = kind;
        this.resourceOperator = resourceOperator;
    }

    @Override
    public String kind() {
        return kind;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code namespace} and
     * cluster {@code name}
     *
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    private String getLockName(String namespace, String name) {
        return "lock::" + namespace + "::" + kind() + "::" + name;
    }

    /**
     * Asynchronously creates or updates the given {@code resource}.
     * This method can be called when the given {@code resource} has been created,
     * or updated and also at some regular interval while the resource continues to exist in Kubernetes.
     * The calling of this method does not imply that anything has actually changed.
     * @param reconciliation Uniquely identifies the reconciliation itself.
     * @param resource The resource to be created, or updated.
     * @return A Future which is completed once the reconciliation of the given resource instance is complete.
     */
    protected abstract Future<Void> createOrUpdate(Reconciliation reconciliation, T resource);

    /**
     * Asynchronously deletes the resource identified by {@code reconciliation}.
     * Operators which only create other Kubernetes resources in order to honour their source resource can rely
     * on Kubernetes Garbage Collection to handle deletion.
     * Such operators should return a Future which completes with {@code false}.
     * Operators which handle deletion themselves should return a Future which completes with {@code true}.
     * @param reconciliation
     * @return
     */
    protected abstract Future<Boolean> delete(Reconciliation reconciliation);

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaUser})
     * in the given namespace with the given name and
     * comparing with the corresponding resource.
     * @param reconciliation The reconciliation.
     * @return A Future which is completed with the result of the reconciliation.
     */
    @Override
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
                    T cr = resourceOperator.get(namespace, name);
                    if (cr != null) {
                        validate(cr);
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
                                if (deleteResult.result()) {
                                    log.info("{}: {} {} deleted", reconciliation, kind, name);
                                } else {
                                    log.info("{}: Assembly {} should be deleted by garbage collection", reconciliation, name);
                                }
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                handler.handle(Future.succeededFuture());
                            } else {
                                log.error("{}: Deletion of {} {} failed", reconciliation, kind, name, deleteResult.cause());
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                handler.handle(Future.failedFuture(deleteResult.cause()));
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
        Future<Void> result = Future.future();
        handler.setHandler(reconcileResult -> {
            handleResult(reconciliation, reconcileResult);
            result.handle(reconcileResult);
        });
        return result;
    }

    protected <T> Future<T> async(Supplier<T> supplier) {
        Future<T> result = Future.future();
        vertx.executeBlocking(
            future -> {
                try {
                    future.complete(supplier.get());
                } catch (Throwable t) {
                    future.fail(t);
                }
            }, result
        );
        return result;
    }

    /**
     * Validate the Custom Resource.
     * This should log at the WARN level (rather than throwing)
     * if the resource can safely be reconciled (e.g. it merely using deprecated API).
     * @param resource The custom resource
     * @throws InvalidResourceException if the resource cannot be safely reconciled.
     */
    protected void validate(T resource) {
        if (resource != null) {
            ResourceVisitor.visit(resource, new ValidationVisitor(resource, log));
        }
    }

    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return resourceOperator.listAsync(namespace, selector())
                .map(resourceList ->
                        resourceList.stream()
                                .map(resource -> new NamespaceAndName(resource.getMetadata().getNamespace(), resource.getMetadata().getName()))
                                .collect(Collectors.toSet()));
    }

    /**
     * A selector to narrow the scope of the {@linkplain #createWatch(String, Consumer) watch}
     * and {@linkplain #allResourceNames(String) query}.
     * @return A selector.
     */
    public Optional<LabelSelector> selector() {
        return Optional.empty();
    }

    /**
     * Create Kubernetes watch for KafkaUser resources.
     *
     * @param namespace Namespace where to watch for users.
     * @param onClose Callback called when the watch is closed.
     *
     * @return A future which completes when the watcher has been created.
     */
    public Future<Watch> createWatch(String namespace, Consumer<KubernetesClientException> onClose) {
        return async(() -> resourceOperator.watch(namespace, selector(), new OperatorWatcher<>(this, namespace, onClose)));
    }

    public Consumer<KubernetesClientException> recreateWatch(String namespace) {
        Consumer<KubernetesClientException> kubernetesClientExceptionConsumer = new Consumer<KubernetesClientException>() {
            @Override
            public void accept(KubernetesClientException e) {
                if (e != null) {
                    log.error("Watcher closed with exception in namespace {}", namespace, e);
                    createWatch(namespace, this);
                } else {
                    log.info("Watcher closed in namespace {}", namespace);
                }
            }
        };
        return kubernetesClientExceptionConsumer;
    }

    /**
     * Log the reconciliation outcome.
     */
    private void handleResult(Reconciliation reconciliation, AsyncResult<Void> result) {
        if (result.succeeded()) {
            log.info("{}: reconciled", reconciliation);
        } else {
            Throwable cause = result.cause();
            if (cause instanceof InvalidConfigParameterException) {
                log.warn("{}: Failed to reconcile {}", reconciliation, cause.getMessage());
            } else {
                log.warn("{}: Failed to reconcile", reconciliation, cause);
            }
        }
    }

}
