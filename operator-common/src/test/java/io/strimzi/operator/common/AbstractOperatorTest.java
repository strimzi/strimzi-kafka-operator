/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractWatchableStatusedNamespacedResourceOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.Lock;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
class AbstractOperatorTest {
    private static Vertx vertx;
    private static final String EXPECTED_MESSAGE = "Exception is expected";

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx(new VertxOptions()
                .setBlockedThreadCheckInterval(60)
                .setBlockedThreadCheckIntervalUnit(TimeUnit.SECONDS)
        );
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    /**
     * Verifies that the lock is released by a call to `releaseLockAndTimer`. 
     * The call is made through a chain of futures ending with `eventually` after a normal/successful execution of the `Callable`
     */
    void testWithLockCallableSuccessfulReleasesLock(VertxTestContext context) throws Exception {
        var resourceOperator = new DefaultWatchableStatusedResourceOperator<>(vertx, null, "TestResource");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        var target = new DefaultOperator(vertx, "Test", resourceOperator, new MicrometerMetricsProvider(), null);
        Reconciliation reconciliation = new Reconciliation("test", "TestResource", "my-namespace", "my-resource");
        String lockName = target.getLockName(reconciliation);

        Checkpoint callableSucceeded = context.checkpoint();
        Checkpoint lockObtained  = context.checkpoint();
        @SuppressWarnings("unchecked")
        Future<String> result = target.withLockTest(reconciliation, () -> Future.succeededFuture("OK"));
        Promise<Void> successHandlerCalled = Promise.promise();

        result.onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(v, is("OK"));
            successHandlerCalled.complete();
            callableSucceeded.flag();
        })));

        successHandlerCalled.future()
            .compose(v -> vertx.sharedData().getLockWithTimeout(lockName, 10000L))
            .onComplete(context.succeeding(lock -> context.verify(() -> {
                assertThat(lock, instanceOf(Lock.class));
                lock.release();
                lockObtained.flag();
            })));
    }

    @Test
    /**
     * Verifies that the lock is released by a call to `releaseLockAndTimer`. 
     * The call is made through a chain of futures ending with `eventually` after a failed execution via a handled exception in the `Callable`.
     */
    void testWithLockCallableHandledExceptionReleasesLock(VertxTestContext context) throws Exception {
        var resourceOperator = new DefaultWatchableStatusedResourceOperator<>(vertx, null, "TestResource");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        var target = new DefaultOperator(vertx, "Test", resourceOperator, new MicrometerMetricsProvider(), null);
        Reconciliation reconciliation = new Reconciliation("test", "TestResource", "my-namespace", "my-resource");
        String lockName = target.getLockName(reconciliation);

        Checkpoint callableFailed = context.checkpoint();
        Checkpoint lockObtained  = context.checkpoint();
        @SuppressWarnings("unchecked")
        Future<String> result = target.withLockTest(reconciliation,
                () -> Future.failedFuture(new UnsupportedOperationException(EXPECTED_MESSAGE)));

        Promise<Void> failHandlerCalled = Promise.promise();

        result.onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e.getMessage(), is(EXPECTED_MESSAGE));
            failHandlerCalled.complete();
            callableFailed.flag();
        })));

        failHandlerCalled.future()
            .compose(nothing -> vertx.sharedData().getLockWithTimeout(lockName, 10000L))
            .onComplete(context.succeeding(lock -> context.verify(() -> {
                assertThat(lock, instanceOf(Lock.class));
                lock.release();
                lockObtained.flag();
            })));
    }

    @Test
    /**
     * Verifies that the lock is released by a call to `releaseLockAndTimer`.
     * The call is made through a chain of futures ending with `eventually` after a failed execution via an unhandled exception in the `Callable`.
     */
    void testWithLockCallableUnhandledExceptionReleasesLock(VertxTestContext context) throws Exception {
        var resourceOperator = new DefaultWatchableStatusedResourceOperator<>(vertx, null, "TestResource");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        var target = new DefaultOperator(vertx, "Test", resourceOperator, new MicrometerMetricsProvider(), null);
        Reconciliation reconciliation = new Reconciliation("test", "TestResource", "my-namespace", "my-resource");
        String lockName = target.getLockName(reconciliation);

        Checkpoint callableFailed = context.checkpoint();
        Checkpoint lockObtained  = context.checkpoint();
        @SuppressWarnings("unchecked")
        Future<String> result = target.withLockTest(reconciliation,
                () -> {
                    throw new UnsupportedOperationException(EXPECTED_MESSAGE);
                });

        Promise<Void> failHandlerCalled = Promise.promise();

        result.onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e.getMessage(), is(EXPECTED_MESSAGE));
            failHandlerCalled.complete();
            callableFailed.flag();
        })));

        failHandlerCalled.future()
            .compose(nothing -> vertx.sharedData().getLockWithTimeout(lockName, 10000L))
            .onComplete(context.succeeding(lock -> context.verify(() -> {
                assertThat(lock, instanceOf(Lock.class));
                lock.release();
                lockObtained.flag();
            })));
    }

    @Test
    /**
     * Verifies that lock is released by call to `releaseLockAndTimer`. 
     * The call is made through a chain of futures ending with `eventually` after a failed execution via an unhandled exception in the `Callable`, 
     * followed by an unhandled exception occurring in the `onFailure` handler.
     */
    void testWithLockFailHandlerUnhandledExceptionReleasesLock(VertxTestContext context) throws Exception {
        var resourceOperator = new DefaultWatchableStatusedResourceOperator<>(vertx, null, "TestResource");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        var target = new DefaultOperator(vertx, "Test", resourceOperator, new MicrometerMetricsProvider(), null);
        Reconciliation reconciliation = new Reconciliation("test", "TestResource", "my-namespace", "my-resource");
        String lockName = target.getLockName(reconciliation);

        Promise<Void> handlersRegistered = Promise.promise();
        Promise<Void> failHandlerCalled = Promise.promise();

        @SuppressWarnings("unchecked")
        Future<String> result = target.withLockTest(reconciliation,
                // TEST SETUP: Do not throw the exception until all handlers registered
                () -> handlersRegistered.future().compose(nothing -> {
                    throw new UnsupportedOperationException(EXPECTED_MESSAGE);
                }));

        Checkpoint callableFailed = context.checkpoint();
        Checkpoint lockObtained  = context.checkpoint();

        result.onComplete(ar -> {
            assertThat(ar.failed(), is(true));
            Throwable e = ar.cause();
            context.verify(() -> {
                assertThat(e.getMessage(), is(EXPECTED_MESSAGE));
                callableFailed.flag();
            });
            try {
                throw new RuntimeException(e);
            } finally {
                // Enables the subsequent lock retrieval to verify it has been unlocked.
                failHandlerCalled.complete();
            }
        });

        failHandlerCalled.future()
            .compose(nothing ->
                vertx.sharedData().getLockWithTimeout(lockName, 10000L))
            .onComplete(context.succeeding(lock -> context.verify(() -> {
                assertThat(lock, instanceOf(Lock.class));
                lock.release();
                lockObtained.flag();
            })));

        handlersRegistered.complete();
    }

    private static class DefaultOperator<
            T extends CustomResource<P, S>,
            P extends Spec,
            S extends Status,
            O extends AbstractWatchableStatusedNamespacedResourceOperator<?, T, ?, ?>>
                extends AbstractOperator<T, P, S, O> {

        public DefaultOperator(Vertx vertx, String kind, O resourceOperator, MetricsProvider metrics, Labels selectorLabels) {
            super(vertx, kind, resourceOperator, metrics, selectorLabels);
        }

        @Override
        protected Future<S> createOrUpdate(Reconciliation reconciliation, T resource) {
            return null;
        }

        @Override
        protected Future<Boolean> delete(Reconciliation reconciliation) {
            return null;
        }

        @Override
        protected S createStatus() {
            return null;
        }

        public String getLockName(Reconciliation reconciliation) {
            return getLockName(reconciliation.namespace(), reconciliation.name());
        }

        public <C> Future<C> withLockTest(Reconciliation reconciliation, Callable<Future<C>> callable) {
            return withLock(reconciliation, LOCK_TIMEOUT_MS, callable);
        }
    }

    private static class DefaultWatchableStatusedResourceOperator<
            C extends KubernetesClient,
            T extends HasMetadata,
            L extends KubernetesResourceList<T>,
            R extends Resource<T>>
                extends AbstractWatchableStatusedNamespacedResourceOperator<C, T, L, R> {

        public DefaultWatchableStatusedResourceOperator(Vertx vertx, C client, String resourceKind) {
            super(vertx, client, resourceKind);
        }

        @Override
        public Future<T> updateStatusAsync(Reconciliation reconciliation, T resource) {
            return null;
        }

        @Override
        protected MixedOperation<T, L, R> operation() {
            return null;
        }
    }
}
