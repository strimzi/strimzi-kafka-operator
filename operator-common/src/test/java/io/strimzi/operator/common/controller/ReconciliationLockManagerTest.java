/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReconciliationLockManagerTest {
    // Same key, one after another
    @Test
    public void testLockUnlockLockUnlock() throws InterruptedException {
        ReconciliationLockManager lockMan = new ReconciliationLockManager();

        assertThat(lockMan.tryLock("my-lock", 10, TimeUnit.MILLISECONDS), is(true));
        assertThat(lockMan.locks.size(), is(1));
        lockMan.unlock("my-lock");
        assertThat(lockMan.tryLock("my-lock", 10, TimeUnit.MILLISECONDS), is(true));
        assertThat(lockMan.locks.size(), is(1));
        lockMan.unlock("my-lock");

        assertThat(lockMan.locks.size(), is(0)); // Should be empty at the end
    }

    // Parallel lock with different keys
    @Test
    public void testLockLockUnlockUnlock() throws InterruptedException {
        ReconciliationLockManager lockMan = new ReconciliationLockManager();

        assertThat(lockMan.tryLock("my-lock", 10, TimeUnit.MILLISECONDS), is(true));
        assertThat(lockMan.tryLock("my-lock2", 10, TimeUnit.MILLISECONDS), is(true));
        assertThat(lockMan.locks.size(), is(2));
        lockMan.unlock("my-lock");
        lockMan.unlock("my-lock2");

        assertThat(lockMan.locks.size(), is(0)); // Should be empty at the end
    }

    // Same key, waiting for lock
    @Test
    public void testLockLockUnlock() throws InterruptedException {
        ReconciliationLockManager lockMan = new ReconciliationLockManager();

        CountDownLatch locked = new CountDownLatch(1); // Used by the async process to indicate it obtained the lock
        CountDownLatch unlock = new CountDownLatch(1); // Used to tell the async process to unlock the lock
        CountDownLatch unlocked = new CountDownLatch(1); // used by the async process to indicate that the lock was unlocked

        assertThat(lockMan.tryLock("my-lock", 10, TimeUnit.MILLISECONDS), is(true));
        assertThat(lockMan.locks.size(), is(1));

        // Async process to test competing for the lock
        CompletableFuture.runAsync(() -> {
            try {
                // Wait for the lock
                lockMan.tryLock("my-lock", 1_000, TimeUnit.MILLISECONDS);

                // Indicate we got the lock
                locked.countDown();

                // Wait until we are told to unlock
                unlock.await();

                // Unlock
                lockMan.unlock("my-lock");

                // Indicate we unlocked
                unlocked.countDown();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for the async process to be queued
        TestUtils.waitFor(
                "Wait for waiting for lock",
                10,
                1_000,
                () -> lockMan.locks.get("my-lock").lockQueue.get() == 2,
                () -> {
                    throw new RuntimeException("The async is not waiting yet");
                });

        // Unlock our lock
        lockMan.unlock("my-lock");

        // Wait until the async process gets the lock
        locked.await();

        // Tell the async process to unlock
        unlock.countDown();

        // Wait for the async process to actually unlock
        unlocked.await();

        assertThat(lockMan.locks.size(), is(0)); // Should be empty at the end
    }
}
