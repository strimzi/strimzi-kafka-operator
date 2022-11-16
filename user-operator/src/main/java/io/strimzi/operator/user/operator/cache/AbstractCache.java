/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract cache provides a periodically refreshed cache. The cache is based around ConcurrentHashMap and a scheduled
 * periodical timer which regularly updates the cache. It also provides method to access the cache and its data.
 */
public abstract class AbstractCache<T> {
    private final static Logger LOGGER = LogManager.getLogger(AbstractCache.class);

    private final long refreshIntervalMs;
    private final ScheduledExecutorService scheduledExecutor;

    private volatile ConcurrentHashMap<String, T> cache = null;

    /**
     * Constructs the abstract cache
     *
     * @param name                  Name of the cache
     * @param refreshIntervalMs     Interval in which the cache should be refreshed
     */
    public AbstractCache(String name, long refreshIntervalMs) {
        this.refreshIntervalMs = refreshIntervalMs;
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, name + "-cache"));
    }

    /**
     * Method for loading the data into the cache. This method is implemented by the different cache implementations.
     *
     * @return  New map with the latest data
     */
    protected abstract ConcurrentHashMap<String, T> loadCache();

    /**
     * Retrieves a value from the cache for given key.
     *
     * @param key   Key which should be retrieved from the cache
     *
     * @return  Returns the value for given key
     */
    public T get(String key)   {
        return getOrDefault(key, null);
    }

    /**
     * Retrieves a value from the cache for given key or returns the default value if the key doesn't exist in the cache.
     *
     * @param key           Key which should be returned from the cache
     * @param defaultValue  The fallback value which should be returned in case the key is not present in the cache
     *
     * @return  The value from the cache or the default value
     */
    public T getOrDefault(String key, T defaultValue)   {
        if (cache == null)  {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not ready!");
        } else {
            return cache.getOrDefault(key, defaultValue);
        }
    }

    /**
     * Creates or updates a record in the cache
     *
     * @param key       Key for which the value should be created or updated
     * @param value     The new value which should be stored in the cache
     */
    public void put(String key, T value)   {
        if (cache == null)  {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not ready!");
        } else {
            cache.put(key, value);
        }
    }

    /**
     * Removes the key from the cache
     *
     * @param key   The key which should be removed
     */
    public void remove(String key) {
        if (cache == null)  {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not ready!");
        } else {
            cache.remove(key);
        }
    }

    /**
     * Returns all keys which exist in the cache
     *
     * @return  Enumeration with all present keys
     */
    public Enumeration<String> keys()  {
        if (cache == null)  {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not ready!");
        } else {
            return cache.keys();
        }
    }

    /**
     * Starts the cache: this method schedules a time which will periodically refresh the cache
     */
    public void start()  {
        LOGGER.info("Starting {}", this.getClass().getSimpleName());

        initialize();

        scheduledExecutor.scheduleAtFixedRate(this::updateCache, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Initializes the cache: this method calls the update cache method when the cache is started
     */
    private void initialize()   {
        updateCache();
    }

    /**
     * Stops the cache: this method sets the stop flag and interrupt the run loop
     */
    public void stop()  {
        LOGGER.info("Stopping {}", this.getClass().getSimpleName());
        scheduledExecutor.shutdownNow();
        cache = null;
    }

    /**
     * Called periodically to update the cache
     */
    private void updateCache()  {
        try {
            LOGGER.debug("Starting update of {}", this.getClass().getSimpleName());
            cache = loadCache();
            LOGGER.debug("{} updated", this.getClass().getSimpleName());
        } catch (Exception e)   {
            LOGGER.error("{} failed to update", this.getClass().getSimpleName(), e);
            cache = null; // Reset the cache
        }
    }
}
