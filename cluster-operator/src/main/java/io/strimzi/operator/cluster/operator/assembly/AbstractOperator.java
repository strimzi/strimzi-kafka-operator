/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.Watcher;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractWatchableStatusedNamespacedResourceOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.common.metrics.OperatorMetricsHolder;
import io.strimzi.operator.common.model.InvalidConfigParameterException;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A base implementation of {@link Operator}.
 *
 * <ul>
 * <li>uses the Fabric8 kubernetes API and implements
 * {@link #reconcile(Reconciliation)} by delegating to abstract {@link #createOrUpdate(Reconciliation, CustomResource)}
 * and {@link #delete(Reconciliation)} methods for subclasses to implement.
 *
 * <li>add support for operator-side {@linkplain StatusUtils#validate(Reconciliation, CustomResource)} validation}.
 *     This can be used to automatically log warnings about source resources which used deprecated part of the CR API.
 *ą
 * </ul>
 * @param <T> The Java representation of the Kubernetes resource, e.g. {@code Kafka} or {@code KafkaConnect}
 * @param <O> The "Resource Operator" for the source resource type. Typically, this will be some instantiation of
 *           {@link io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator}.
 */
public abstract class AbstractOperator<
        T extends CustomResource<P, S>,
        P extends Spec,
        S extends Status,
        O extends AbstractWatchableStatusedNamespacedResourceOperator<?, T, ?, ?>>
            implements Operator {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractOperator.class);

    private static final long PROGRESS_WARNING = 60_000L;
    protected static final int LOCK_TIMEOUT_MS = 10000;

    protected final Vertx vertx;
    protected final O resourceOperator;
    private final String kind;

    private final LabelSelector selector;

    protected final OperatorMetricsHolder metrics;

    private final Map<String, AtomicInteger> resourcesStateCounter = new ConcurrentHashMap<>(1);

    /**
     * Constructs the AbstractOperator. This constructor is used to construct the AbstractOperator using the
     * OperatorMetricsHolder instance. This constructor is used by subclasses which want to use specialized metrics
     * ¨holder such as the subclasses dealing with KafkaConnector resources and their metrics.
     *
     * @param vertx             Vert.x instance
     * @param kind              Resource kind which will be operated by this operator
     * @param resourceOperator  Resource operator for given custom resource
     * @param metrics           MetricsHolder for managing operator metrics
     * @param selectorLabels    Selector labels for selecting custom resources which should be operated
     */
    public AbstractOperator(Vertx vertx, String kind, O resourceOperator, OperatorMetricsHolder metrics, Labels selectorLabels) {
        this.vertx = vertx;
        this.kind = kind;
        this.resourceOperator = resourceOperator;
        this.selector = (selectorLabels == null || selectorLabels.toMap().isEmpty()) ? null : new LabelSelector(null, selectorLabels.toMap());
        this.metrics = metrics;
    }

    /**
     * Constructs the AbstractOperator. This constructor is used to construct the AbstractOperator using the
     * MetricsProvider instance, which is used to create OperatorMetricsHolder inside the constructor. It is used by
     * subclasses which do not need a more specialized type of metrics holder.
     *
     * @param vertx             Vert.x instance
     * @param kind              Resource kind which will be operated by this operator
     * @param resourceOperator  Resource operator for given custom resource
     * @param metricsProvider   Metrics provider which should be used to create the OperatorMetricsHolder instance
     * @param selectorLabels    Selector labels for selecting custom resources which should be operated
     */
    public AbstractOperator(Vertx vertx, String kind, O resourceOperator, MetricsProvider metricsProvider, Labels selectorLabels) {
        this(vertx, kind, resourceOperator, new OperatorMetricsHolder(kind, selectorLabels, metricsProvider), selectorLabels);
    }

    @Override
    public String kind() {
        return kind;
    }

    @Override
    public OperatorMetricsHolder metrics()   {
        return metrics;
    }

    @Override
    public void removeMetrics(Set<NamespaceAndName> desiredNames, String namespace) {
        // Intentionally left blank for dedicated Kinds to implement, but not be required by.
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code namespace} and
     * cluster {@code name}
     *
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    /* test */ String getLockName(String namespace, String name) {
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
    protected abstract Future<S> createOrUpdate(Reconciliation reconciliation, T resource);

    /**
     * Asynchronously deletes the resource identified by {@code reconciliation}.
     * Operators which only create other Kubernetes resources in order to honour their source resource can rely
     * on Kubernetes Garbage Collection to handle deletion.
     * Such operators should return a Future which completes with {@code false}.
     * Operators which handle deletion themselves should return a Future which completes with {@code true}.
     *
     * @param reconciliation    Reconciliation marker
     *
     * @return  Future which completes when the deletion is complete
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
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        metrics().reconciliationsCounter(reconciliation.namespace()).increment();
        Timer.Sample reconciliationTimerSample = Timer.start(metrics().metricsProvider().meterRegistry());

        Future<Void> handler = withLock(reconciliation, LOCK_TIMEOUT_MS, () ->
            resourceOperator.getAsync(namespace, name)
                .compose(cr -> cr != null ? reconcileResource(reconciliation, cr) : reconcileDeletion(reconciliation)));

        Promise<Void> result = Promise.promise();
        handler.onComplete(reconcileResult ->
            callSafely(reconciliation, () -> handleResult(reconciliation, reconcileResult, reconciliationTimerSample))
                .onComplete(handleSafely(reconciliation, ignored -> result.handle(reconcileResult))));

        return result.future();
    }

    /**
     * Reconcile assembly resources in the namespace given by {@code reconciliation} having the name
     * give by {@code reconciliation}.
     *
     * Reconciliation works by comparing the assembly resource given by the {@code cr} parameter
     * (e.g. a {@code KafkaUser} resource) with the corresponding resource(s) in the cluster.
     *
     * @param reconciliation The reconciliation.
     * @param cr The custom resource
     * @return A Future which is completed with the result of the reconciliation.
     */
    Future<Void> reconcileResource(Reconciliation reconciliation, T cr) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        if (!Util.matchesSelector(selector(), cr))  {
            // When the labels matching the selector are removed from the custom resource, a DELETE event is
            // triggered by the watch even through the custom resource might not match the watch labels anymore
            // and might not be really deleted. We have to filter these situations out and ignore the
            // reconciliation because such resource might be already operated by another instance (where the
            // same change triggered ADDED event).
            LOGGER.debugCr(reconciliation, "{} {} in namespace {} does not match label selector {} and will be ignored", kind(), name, namespace, selector().getMatchLabels());
            return Future.succeededFuture();
        }

        Promise<Void> createOrUpdate = Promise.promise();
        if (Annotations.isReconciliationPausedWithAnnotation(cr)) {
            S status = createStatus(cr);
            Set<Condition> conditions = StatusUtils.validate(reconciliation, cr);
            conditions.add(StatusUtils.getPausedCondition());
            status.setConditions(new ArrayList<>(conditions));
            status.setObservedGeneration(cr.getStatus() != null ? cr.getStatus().getObservedGeneration() : 0);

            updateStatus(reconciliation, status).onComplete(statusResult -> {
                if (statusResult.succeeded()) {
                    createOrUpdate.complete();
                } else {
                    createOrUpdate.fail(statusResult.cause());
                }
            });
            metrics().pausedResourceCounter(namespace).getAndIncrement();
            LOGGER.infoCr(reconciliation, "Reconciliation of {} {} is paused", kind, name);
            return createOrUpdate.future();
        } else if (cr.getSpec() == null) {
            InvalidResourceException exception = new InvalidResourceException("Spec cannot be null");

            S status = createStatus(cr);
            Condition errorCondition = new ConditionBuilder()
                    .withLastTransitionTime(StatusUtils.iso8601Now())
                    .withType("NotReady")
                    .withStatus("True")
                    .withReason(exception.getClass().getSimpleName())
                    .withMessage(exception.getMessage())
                    .build();
            status.setObservedGeneration(cr.getMetadata().getGeneration());
            status.addCondition(errorCondition);

            LOGGER.errorCr(reconciliation, "{} spec cannot be null", cr.getMetadata().getName());
            updateStatus(reconciliation, status).onComplete(notUsed -> createOrUpdate.fail(exception));

            return createOrUpdate.future();
        }

        Set<Condition> unknownAndDeprecatedConditions = StatusUtils.validate(reconciliation, cr);

        LOGGER.infoCr(reconciliation, "{} {} will be checked for creation or modification", kind, name);

        createOrUpdate(reconciliation, cr).onComplete(res -> {
            if (res.succeeded()) {
                S status = res.result();

                StatusUtils.addConditionsToStatus(status, unknownAndDeprecatedConditions);
                updateStatus(reconciliation, status).onComplete(statusResult -> {
                    if (statusResult.succeeded()) {
                        createOrUpdate.complete();
                    } else {
                        createOrUpdate.fail(statusResult.cause());
                    }
                });
            } else if (res.cause() instanceof ReconciliationException e) {
                @SuppressWarnings("unchecked")
                S status = (S) e.getStatus();
                StatusUtils.addConditionsToStatus(status, unknownAndDeprecatedConditions);

                LOGGER.errorCr(reconciliation, "createOrUpdate failed", e.getCause());
                updateStatus(reconciliation, status).onComplete(statusResult -> createOrUpdate.fail(e.getCause()));
            } else {
                LOGGER.errorCr(reconciliation, "createOrUpdate failed", res.cause());
                createOrUpdate.fail(res.cause());
            }
        });

        return createOrUpdate.future();
    }

    /**
     * Delete assembly resources in the namespace given by {@code reconciliation} having the name
     * give by {@code reconciliation}.
     *
     * @param reconciliation The reconciliation.
     * @return A Future which is completed with the result of the reconciliation.
     */
    Future<Void> reconcileDeletion(Reconciliation reconciliation) {
        String name = reconciliation.name();
        LOGGER.infoCr(reconciliation, "{} {} should be deleted", kind, name);

        return delete(reconciliation).<Void>map(deleteResult -> {
            if (deleteResult) {
                LOGGER.infoCr(reconciliation, "{} {} deleted", kind, name);
            } else {
                LOGGER.infoCr(reconciliation, "Assembly {} or some parts of it will be deleted by garbage collection", name);
            }
            return null;
        }).recover(deleteResult -> {
            LOGGER.errorCr(reconciliation, "Deletion of {} {} failed", kind, name, deleteResult);
            return Future.failedFuture(deleteResult);
        });
    }

    /**
     * Updates the Status field of the Kafka CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param reconciliation the reconciliation identified
     * @param desiredStatus The KafkaStatus which should be set
     *
     * @return  Future which completes when the status is updated
     */
    Future<Void> updateStatus(Reconciliation reconciliation, S desiredStatus) {
        if (desiredStatus == null)  {
            LOGGER.debugCr(reconciliation, "Desired status is null - status will not be updated");
            return Future.succeededFuture();
        }

        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        return resourceOperator.getAsync(namespace, name)
                .compose(res -> {
                    if (res != null) {
                        S currentStatus = res.getStatus();
                        StatusDiff sDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!sDiff.isEmpty()) {
                            res.setStatus(desiredStatus);

                            return resourceOperator.updateStatusAsync(reconciliation, res)
                                    .compose(notUsed -> {
                                        LOGGER.debugCr(reconciliation, "Completed status update");
                                        return Future.succeededFuture();
                                    }, error -> {
                                            LOGGER.errorCr(reconciliation, "Failed to update status", error);
                                            return Future.failedFuture(error);
                                        });
                        } else {
                            LOGGER.debugCr(reconciliation, "Status did not change");
                            return Future.succeededFuture();
                        }
                    } else {
                        LOGGER.errorCr(reconciliation, "Current {} resource not found", reconciliation.kind());
                        return Future.failedFuture("Current " + reconciliation.kind() + " resource with name " + name + " not found");
                    }
                }, error -> {
                        LOGGER.errorCr(reconciliation, "Failed to get the current {} resource and its status", reconciliation.kind(), error);
                        return Future.failedFuture(error);
                    });
    }

    protected abstract S createStatus(T cr);

    /**
     * The exception by which Futures returned by {@link #withLock(Reconciliation, long, Callable)} are failed when
     * the lock cannot be acquired within the timeout.
     */
    static class UnableToAcquireLockException extends TimeoutException { }

    /**
     * Acquire the lock for the resource implied by the {@code reconciliation}
     * and call the given {@code callable} with the lock held.
     * Once the callable returns (or if it throws) release the lock and complete the returned Future.
     * If the lock cannot be acquired the given {@code callable} is not called and the returned Future is completed with {@link UnableToAcquireLockException}.
     *
     * @param reconciliation    Reconciliation marker
     * @param callable          Function which will be called when the lock is acquired
     *
     * @param <T>   Type of the custom resource managed by this operator
     *
     * @return  Future which completes when the callable is completed.
     */
    protected final <T> Future<T> withLock(Reconciliation reconciliation, long lockTimeoutMs, Callable<Future<T>> callable) {
        Promise<T> handler = Promise.promise();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        final String lockName = getLockName(namespace, name);
        LOGGER.debugCr(reconciliation, "Try to acquire lock {}", lockName);
        vertx.sharedData().getLockWithTimeout(lockName, lockTimeoutMs, res -> {
            if (res.succeeded()) {
                LOGGER.debugCr(reconciliation, "Lock {} acquired", lockName);

                Lock lock = res.result();
                long timerId = vertx.setPeriodic(PROGRESS_WARNING, timer -> LOGGER.infoCr(reconciliation, "Reconciliation is in progress"));

                callSafely(reconciliation, callable)
                    .onSuccess(handleSafely(reconciliation, handler::complete))
                    .onFailure(handleSafely(reconciliation, handler::fail))
                    .eventually(() -> releaseLockAndTimer(reconciliation, lock, lockName, timerId));
            } else {
                LOGGER.debugCr(reconciliation, "Failed to acquire lock {} within {}ms.", lockName, lockTimeoutMs);
                handler.fail(new UnableToAcquireLockException());
            }
        });
        return handler.future();
    }

    /**
     * Safely executes <code>callable</code>, always returning a
     * <code>Future</code>. In the context of this method, the term "safely"
     * indicates that exceptions thrown by <code>callable</code> will result in the
     * return of a failed <code>Future</code>.
     *
     * @param <C>            result type of the <code>Future</code> returned by the
     *                       provided <code>callable</code>
     * @param reconciliation the reconciliation being processed by
     *                       <code>callable</code>
     * @param callable       callable routine for processing the reconciliation
     *
     * @return the result of <code>callable</code> or a failed <code>Future</code>
     *         when an exception is thrown
     */
    private <C> Future<C> callSafely(Reconciliation reconciliation, Callable<Future<C>> callable) {
        try {
            return callable.call();
        } catch (Throwable ex) {
            LOGGER.errorCr(reconciliation, "Reconciliation failed", ex);
            return Future.failedFuture(ex);
        }
    }

    /**
     * Provides a proxy <code>Handler</code> that will safely execute the given
     * <code>handler</code>. In the context of this method, the term "safely"
     * indicates that exceptions thrown by <code>handler</code> will be caught and
     * logged, and not thrown to the caller.
     *
     * @param <H>            argument type of the <code>Handler</code>
     * @param reconciliation the reconciliation being processed, the context of the
     *                       operation
     * @param handler        handler, for either success or failure
     *
     * @return a proxy <code>Handler</code> that, when executed, will execute the
     *         provided handler, ensuring that unhandled exceptions are caught and
     *         logged.
     */
    private <H> Handler<H> handleSafely(Reconciliation reconciliation, Handler<H> handler) {
        return result -> {
            try {
                handler.handle(result);
            } catch (Throwable ex) {
                LOGGER.errorCr(reconciliation, "Reconciliation completion handler failed", ex);
            }
        };
    }

    private Future<Void> releaseLockAndTimer(Reconciliation reconciliation, Lock lock, String lockName, long timerId) {
        vertx.cancelTimer(timerId);
        lock.release();
        LOGGER.debugCr(reconciliation, "Lock {} released", lockName);
        return Future.succeededFuture();
    }

    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return resourceOperator.listAsync(namespace, selector())
                .map(resourceList ->
                        resourceList.stream()
                                .map(resource -> new NamespaceAndName(resource.getMetadata().getNamespace(), resource.getMetadata().getName()))
                                .collect(Collectors.toSet()));
    }

    /**
     * A selector to narrow the scope of the {@linkplain #createWatch(String) watch}
     * and {@linkplain #allResourceNames(String) query}.
     * @return A selector.
     */
    public LabelSelector selector() {
        return selector;
    }

    /**
     * Create Kubernetes watch
     *
     * @param namespace     Namespace where to watch for resources
     *
     * @return  A future which completes when the watcher has been created
     */
    public Future<ReconnectingWatcher<T>> createWatch(String namespace) {
        return VertxUtil.async(vertx, () -> new ReconnectingWatcher<>(resourceOperator, kind(), namespace, selector(), this::eventHandler));
    }

    /**
     * Event handler called when the watch receives an event.
     *
     * @param action    An Action describing the type of the event
     * @param resource  The resource for which the event was triggered
     */
    private void eventHandler(Watcher.Action action, T resource) {
        String name = resource.getMetadata().getName();
        String namespace = resource.getMetadata().getNamespace();

        switch (action) {
            case ADDED, DELETED, MODIFIED -> {
                Reconciliation reconciliation = new Reconciliation("watch", this.kind(), namespace, name);
                LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", this.kind(), name, namespace, action);
                reconcile(reconciliation);
            }
            case ERROR -> {
                LOGGER.errorCr(new Reconciliation("watch", this.kind(), namespace, name), "Failed {} {} in namespace{} ", this.kind(), name, namespace);
                reconcileAll("watch error", namespace, ignored -> {
                });
            }
            default -> {
                LOGGER.errorCr(new Reconciliation("watch", this.kind(), namespace, name), "Unknown action: {} in namespace {}", name, namespace);
                reconcileAll("watch unknown", namespace, ignored -> {
                });
            }
        }
    }

    /**
     * Log the reconciliation outcome.
     */
    private Future<Void> handleResult(Reconciliation reconciliation, AsyncResult<Void> result, Timer.Sample reconciliationTimerSample) {
        Promise<Void> handlingResult = Promise.promise();

        if (result.succeeded()) {
            updateResourceState(reconciliation, true, null).onComplete(stateUpdateResult -> {
                metrics().successfulReconciliationsCounter(reconciliation.namespace()).increment();
                reconciliationTimerSample.stop(metrics().reconciliationsTimer(reconciliation.namespace()));
                LOGGER.infoCr(reconciliation, "reconciled");
                handlingResult.handle(stateUpdateResult);
            });
        } else {
            Throwable cause = result.cause();

            if (cause instanceof InvalidConfigParameterException) {
                updateResourceState(reconciliation, false, cause).onComplete(stateUpdateResult -> {
                    metrics().failedReconciliationsCounter(reconciliation.namespace()).increment();
                    reconciliationTimerSample.stop(metrics().reconciliationsTimer(reconciliation.namespace()));
                    LOGGER.warnCr(reconciliation, "Failed to reconcile {}", cause.getMessage());
                    handlingResult.handle(stateUpdateResult);
                });
            } else if (cause instanceof UnableToAcquireLockException) {
                metrics().lockedReconciliationsCounter(reconciliation.namespace()).increment();
                handlingResult.complete();
            } else {
                updateResourceState(reconciliation, false, cause).onComplete(stateUpdateResult -> {
                    metrics().failedReconciliationsCounter(reconciliation.namespace()).increment();
                    reconciliationTimerSample.stop(metrics().reconciliationsTimer(reconciliation.namespace()));
                    LOGGER.warnCr(reconciliation, "Failed to reconcile", cause);
                    handlingResult.handle(stateUpdateResult);
                });
            }
        }

        return handlingResult.future();
    }

    /**
     * Updates the resource state metric for the provided reconciliation which brings kind, name and namespace
     * of the custom resource.
     *
     * @param reconciliation reconciliation to use to update the resource state metric
     * @param ready if reconcile was successful and the resource is ready
     */
    private Future<Void> updateResourceState(Reconciliation reconciliation, boolean ready, Throwable cause) {
        String key = reconciliation.namespace() + ":" + reconciliation.kind() + "/" + reconciliation.name();

        String errorReason = "none";
        if (cause != null) {
            if (cause.getMessage() != null) {
                errorReason = cause.getMessage();
            } else {
                errorReason = "unknown error";
            }
        }

        Tags metricTags = Tags.of(
                Tag.of("kind", reconciliation.kind()),
                Tag.of("name", reconciliation.name()),
                Tag.of("resource-namespace", reconciliation.namespace()),
                Tag.of("reason", errorReason));

        boolean removed = metrics().removeMetric(MetricsHolder.METRICS_RESOURCE_STATE,
                Tags.of(Tag.of("kind", reconciliation.kind()),
                        Tag.of("name", reconciliation.name()),
                        Tag.of("resource-namespace", reconciliation.namespace())));

        if (removed) {
            resourcesStateCounter.remove(key);
            LOGGER.debugCr(reconciliation, "Removed metric " + MetricsHolder.METRICS_PREFIX + "resource.state{}", key);
        }

        return resourceOperator.getAsync(reconciliation.namespace(), reconciliation.name()).map(cr -> {
            if (cr != null && Util.matchesSelector(selector(), cr)) {
                resourcesStateCounter.computeIfAbsent(key, tags ->
                        metrics().metricsProvider().gauge(MetricsHolder.METRICS_RESOURCE_STATE, "Current state of the resource: 1 ready, 0 fail", metricTags)
                );
                resourcesStateCounter.get(key).set(ready ? 1 : 0);
                LOGGER.debugCr(reconciliation, "Updated metric " + MetricsHolder.METRICS_PREFIX + "resource.state{} = {}", metricTags, ready ? 1 : 0);
            }

            return null;
        });
    }
}
