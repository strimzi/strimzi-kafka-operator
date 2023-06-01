/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.future.FutureInternal;
import io.vertx.core.impl.future.Listener;

/**
 * Custom {@linkplain CompletableFuture} that also implements the Vert.x
 * {@linkplain FutureInternal} interface. May be used as a drop-in for either
 * type, with the intention of helping transition from Vert.x to the Java
 * concurrency APIs. Implements FutureInternal rather than just Future because
 * some Vert.x code assumes (and casts) Futures to FutureInternal.
 *
 * @param <T> The result type returned by this future's {@code join} and
 *            {@code get} methods
 *
 * @see CompletableFuture
 * @see Future
 * @see FutureInternal
 */
public class StrimziFuture<T> extends CompletableFuture<T> implements FutureInternal<T> {

    private static final Executor ASYNC_POOL = new ForkJoinPool(20);
    private static final StrimziFuture<?> EMPTY = completedFuture(null);

    /**
     * Returns a new StrimziFuture that is already completed with
     * the given value.
     *
     * @param value the value
     * @param <U> the type of the value
     * @return the completed StrimziFuture
     *
     * @see CompletableFuture#completedFuture(Object)
     */
    public static <U> StrimziFuture<U> completedFuture(U value) {
        StrimziFuture<U> future = new StrimziFuture<>();
        future.complete(value);
        return future;
    }

    /**
     * Returns a StrimziFuture that is already completed with the null value.
     *
     * @param <U> the type of the value
     * @return the completed StrimziFuture
     */
    @SuppressWarnings("unchecked")
    public static <U> StrimziFuture<U> completedFuture() {
        return (StrimziFuture<U>) EMPTY;
    }

    /**
     * Returns a new StrimziFuture that is already completed
     * exceptionally with the given exception.
     *
     * @param ex the exception
     * @param <U> the type of the value
     * @return the exceptionally completed StrimziFuture
     */
    public static <U> StrimziFuture<U> failedFuture(Throwable ex) {
        Objects.requireNonNull(ex);
        StrimziFuture<U> future = new StrimziFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Returns a new StrimziFuture that is already completed
     * exceptionally with a throwable having the given message.
     *
     * @param message a message used to construct a throwable
     * @param <U> the type of the value
     * @return the exceptionally completed StrimziFuture
     */
    public static <U> StrimziFuture<U> failedFuture(String message) {
        Objects.requireNonNull(message);
        return failedFuture(new NoStackTraceThrowable(message));
    }

    /**
     * Returns a new StrimziFuture that is completed when all of the given
     * CompletionStages complete. This method behaves the same as
     * {@linkplain CompletableFuture#allOf(CompletableFuture...)}, but the returned
     * CompletableFuture is a StrimziFuture.
     *
     * @param stages the CompletionStages
     * @return a new StrimziFuture that is completed when all of the given
     *         CompletionStages complete
     */
    public static StrimziFuture<Void> allOf(Collection<? extends CompletionStage<?>> stages) {
        return allOf(stages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
    }

    /**
     * Returns a new StrimziFuture that is completed when all of the given
     * CompletionStages complete. This method behaves the same as
     * {@linkplain CompletableFuture#allOf(CompletableFuture...)}, but the returned
     * CompletableFuture is a StrimziFuture.
     *
     * @param stages the CompletionStages
     * @return a new StrimziFuture that is completed when all of the given
     *         CompletionStages complete
     */
    public static StrimziFuture<Void> allOf(CompletionStage<?>... stages) {
        return allOf(Arrays.asList(stages));
    }

    /**
     * Returns a new StrimziFuture that is completed when all of the given
     * CompletableFutures complete. This method behaves the same as
     * {@linkplain CompletableFuture#allOf(CompletableFuture...)}, but the returned
     * CompletableFuture is a StrimziFuture.
     *
     * @param futures the CompletableFutures
     * @return a new StrimziFuture that is completed when all of the given
     *         CompletableFutures complete
     */
    public static StrimziFuture<Void> allOf(CompletableFuture<?>... futures) {
        StrimziFuture<Void> promise = new StrimziFuture<>();
        CompletableFuture.allOf(futures).whenComplete((nothing, error) -> {
            if (error != null) {
                promise.completeExceptionally(error);
            } else {
                promise.complete(null);
            }
        });

        return promise;
    }

    /**
     * Returns a new Executor that submits a task to the Strimzi fork/join executor
     * after the given delay (or no delay if non-positive). Each delay commences
     * upon invocation of the returned executor's {@code execute} method.
     *
     * @param delay how long to delay, in units of {@code unit}
     * @param unit  a {@code TimeUnit} determining how to interpret the
     *              {@code delay} parameter
     * @return the new delayed executor
     * @see CompletableFuture#delayedExecutor(long, TimeUnit, Executor)
     */
    public static Executor delayedExecutor(long delay, TimeUnit unit) {
        return CompletableFuture.delayedExecutor(delay, unit, ASYNC_POOL);
    }

    /**
     * Returns a new StrimziFuture that is asynchronously completed by a task
     * running in the Strimzi fork/join executor after it runs the given action.
     *
     * @param runnable the action to run before completing the returned
     *                 StrimziFuture
     * @return the new StrimziFuture
     */
    public static StrimziFuture<Void> runAsync(Runnable runnable) {
        return supplyAsync(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Returns a new StrimziFuture that is asynchronously completed by a task
     * running in the Strimzi fork/join executor with the value obtained by calling
     * the given Supplier.
     *
     * @param supplier a function returning the value to be used to complete the
     *                 returned StrimziFuture
     * @param <U>      the function's return type
     * @return the new StrimziFuture
     */
    public static <U> StrimziFuture<U> supplyAsync(Supplier<U> supplier) {
        return new StrimziFuture<U>().completeAsync(supplier);
    }

    // Instance fields and methods

    final AtomicReference<FutureInternal<T>> vertxFutureRef = new AtomicReference<>();

    FutureInternal<T> vertxFuture() {
        return vertxFutureRef.updateAndGet(value -> value != null ? value : (FutureInternal<T>) Util.toFuture(this));
    }

    // Override CompletableFuture methods for custom async executor pool

    @Override
    public Executor defaultExecutor() {
        return ASYNC_POOL;
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new StrimziFuture<>();
    }

    @Override
    public StrimziFuture<T> completeAsync(Supplier<? extends T> supplier) {
        return (StrimziFuture<T>) super.completeAsync(supplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> StrimziFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return (StrimziFuture<U>) super.thenApply(fn);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> StrimziFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return (StrimziFuture<U>) super.thenApplyAsync(fn);
    }

    @Override
    public <U> StrimziFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return (StrimziFuture<U>) super.thenCompose(fn);
    }

    @Override
    public <U> StrimziFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return (StrimziFuture<U>) super.thenComposeAsync(fn);
    }

    // Methods for io.vertx.core.impl.future.FutureInternal

    @Override
    public ContextInternal context() {
        return vertxFuture().context();
    }

    @Override
    public void addListener(Listener<T> listener) {
        vertxFuture().addListener(listener);
    }

    // Methods for io.vertx.core.Future below

    @Override
    public boolean isComplete() {
        return vertxFuture().isComplete();
    }

    @Override
    public Future<T> onComplete(Handler<AsyncResult<T>> handler) {
        return vertxFuture().onComplete(handler);
    }

    @Override
    public T result() {
        return vertxFuture().result();
    }

    @Override
    public Throwable cause() {
        return vertxFuture().cause();
    }

    @Override
    public boolean succeeded() {
        return vertxFuture().succeeded();
    }

    @Override
    public boolean failed() {
        return vertxFuture().failed();
    }

    @Override
    public <U> Future<U> compose(Function<T, Future<U>> successMapper, Function<Throwable, Future<U>> failureMapper) {
        return vertxFuture().compose(successMapper, failureMapper);
    }

    @Override
    public <U> Future<U> transform(Function<AsyncResult<T>, Future<U>> mapper) {
        return vertxFuture().transform(mapper);
    }

    @Override
    public <U> Future<T> eventually(Function<Void, Future<U>> mapper) {
        return vertxFuture().eventually(mapper);
    }

    @Override
    public <U> Future<U> map(Function<T, U> mapper) {
        return vertxFuture().map(mapper);
    }

    @Override
    public <V> Future<V> map(V value) {
        return vertxFuture().map(value);
    }

    @Override
    public Future<T> otherwise(Function<Throwable, T> mapper) {
        return vertxFuture().otherwise(mapper);
    }

    @Override
    public Future<T> otherwise(T value) {
        return vertxFuture().otherwise(value);
    }

    static class NoStackTraceThrowable extends Throwable {
        private static final long serialVersionUID = 1L;

        public NoStackTraceThrowable(String message) {
            super(message, null, false, false);
        }
    }
}
