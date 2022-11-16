/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AbstractBatchReconcilerTest {
    Set<Integer> reconciled;
    CountDownLatch reconciliationFinished;

    @Test
    public void testBatching() throws InterruptedException {
        int numberOfItems = 15;

        reconciled = new HashSet<>(numberOfItems);
        reconciliationFinished = new CountDownLatch(numberOfItems);

        AbstractBatchReconciler<Integer> batcher = new TestBatchReconciler(20, 5, 100);
        batcher.start();

        Thread producer = new Thread(() -> {
            for (int i = 0; i < numberOfItems; i++)    {
                try {
                    batcher.enqueue(i);
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    System.out.println("TimedOut");
                }
            }
        });
        producer.start();

        reconciliationFinished.await(1_000, TimeUnit.MILLISECONDS);

        for (int i = 0; i < numberOfItems; i++)    {
            MatcherAssert.assertThat(reconciled.contains(i), CoreMatchers.is(true));
        }

        producer.interrupt();
        batcher.stop();
    }

    class TestBatchReconciler extends AbstractBatchReconciler<Integer> {
        public TestBatchReconciler(int queueSize, int maxBatchSize, int maxBatchTime) {
            super("TestBatchReconciler", queueSize, maxBatchSize, maxBatchTime);
        }

        @Override
        protected void reconcile(Collection<Integer> items) {
            reconciled.addAll(items);
            reconciliationFinished.countDown();
        }
    }
}
