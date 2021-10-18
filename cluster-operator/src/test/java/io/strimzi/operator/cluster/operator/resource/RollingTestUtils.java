/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Future;
import org.junit.jupiter.api.Assertions;

public class RollingTestUtils {
    private RollingTestUtils() { }

    public static <T> T await(Future<T> future) {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(ar -> latch.countDown());
        try {
            if (latch.await(1, TimeUnit.SECONDS)) {
                if (future.failed()) {
                    Assertions.fail(future.cause());
                }
                return future.result();
            } else {
                Assertions.fail("Future wasn't completed within timeout");
                throw new RuntimeException(); // to appease definite return checking
            }
        } catch (InterruptedException e) {
            throw new KafkaAvailabilityTest.UncheckedInterruptedException(e);
        }
    }

    public static Throwable awaitThrows(Future<?> future) {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(ar -> latch.countDown());
        try {
            if (latch.await(1, TimeUnit.SECONDS)) {
                if (future.succeeded()) {
                    Assertions.fail("Expected future to have failed");
                }
                return future.cause();
            } else {
                Assertions.fail("Future wasn't completed within timeout");
                throw new RuntimeException(); // to appease definite return checking
            }
        } catch (InterruptedException e) {
            throw new KafkaAvailabilityTest.UncheckedInterruptedException(e);
        }
    }

}
