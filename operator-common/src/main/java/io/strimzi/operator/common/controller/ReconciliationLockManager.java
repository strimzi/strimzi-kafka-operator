/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple lock manager used to track the reconciliations which are in progress. This is used to make sure that a given
 * resource is not reconciled multiple times in parallel.
 *
 * This implementation is inspired by https://www.baeldung.com/java-acquire-lock-by-key
 */
public class ReconciliationLockManager {
    private static final Logger LOGGER = LogManager.getLogger(ReconciliationLockManager.class);

    /*test*/ final ConcurrentHashMap<String, ReconciliationLock> locks = new ConcurrentHashMap<>();

    /**
     * Tries to lock the lock for given key. The key is either obtained from the map if it already exists or created.
     * When getting the existing lock from the map, it increases the counter of the interested parties directly in the
     * locks.compute(...) method to ensure atomicity.
     *
     * @param key   The key for which the lock should be obtained
     * @param time  How many units of time should we wait for the lock
     * @param unit  How long the unit of waiting is
     *
     * @return  True if the lock was successfully obtained. False otherwise
     *
     * @throws InterruptedException Throws in InterruptedException in case interrupted while waiting for the lock
     */
    public boolean tryLock(String key, long time, TimeUnit unit) throws InterruptedException {
        ReconciliationLock rLock = locks.compute(key, (k, v) -> v == null ? new ReconciliationLock() : v.incrementQueueAndGet());
        LOGGER.debug("Trying to obtain lock {}", key);
        return rLock.tryLock(time, unit);
    }


    /**
     * Unlocks the lock for given key. The unlocking happens within the locks.compute(...) call to ensure the
     * atomicity. If there is nobody queued for the lock, the lock is removed from the map to not keep it forever.
     *
     * @param key   The key of the lock which should be unlocked
     */
    public void unlock(String key)    {
        locks.compute(key, (k, v) -> {
            if (v == null)  {
                LOGGER.warn("Lock with key {} does not exist and cannot be unlocked", key);
                return null;
            } else {
                LOGGER.debug("Trying to release lock {}", key);

                if (v.unlock() == 0)    {
                    LOGGER.debug("Lock {} is not in use anymore and will be removed", key);
                    return null;
                } else {
                    return v;
                }
            }
        });
    }

    /**
     * Internal implementation of a reconciliation lock. It holds the lock as well as a counter which is used to track
     * how many parties are interested in the lock (either hold the lock or are waiting for it). The counter is used to
     * detect when the lock is not used anymore and should be removed from the lock manager.
     */
    public static class ReconciliationLock    {
        private final Lock lock = new ReentrantLock();
        /*test*/ final AtomicInteger lockQueue = new AtomicInteger(1); // Initializes at 1, because it is created as part of an tryLock() call

        private ReconciliationLock incrementQueueAndGet()   {
            lockQueue.incrementAndGet();
            return this;
        }

        /**
         * Tries to obtain the lock. If the lock cannot be obtained in given time interval, it returns false. The
         * counter for interested parties is increased either when initializing the object or when getting it from the
         * map. So in this method, we take care only of decreasing it in case we fail to acquire the lock.
         *
         * @param time  How many units of time should we wait for the lock
         * @param unit  How long the unit of waiting is
         *
         * @return  True if the lock was locked. False otherwise.
         *
         * @throws InterruptedException Throws InterruptedException if interrupted while waiting for the lock
         */
        private boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            try {
                boolean locked = lock.tryLock(time, unit);

                if (!locked) {
                    // We did not get the lock and return false => we decrement the counter of interested parties
                    lockQueue.decrementAndGet();
                }

                return locked;
            } catch (InterruptedException e)    {
                // If the waiting for the lock was interrupted, we did not get it. So we decrement the counter and re-throw the exception
                lockQueue.decrementAndGet();
                throw e;
            }
        }

        /**
         * Releases the lock and returns an integer indicating if someone is still interested in the lock. The returned
         * integer is used to determine if the lock can be deleted from the map.
         *
         * @return  Number of parties waiting for this lock
         */
        private int unlock()   {
            lock.unlock();
            return lockQueue.decrementAndGet();
        }
    }
}
