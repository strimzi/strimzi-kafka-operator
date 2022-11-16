/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractCacheTest {
    private CountDownLatch reload;

    @Test
    public void testCache() throws InterruptedException {
        reload = new CountDownLatch(2);

        AbstractCache<Boolean> cache = new TestCache();

        // Check the initial state before start
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> cache.get("one"));
        assertThat(e.getMessage(), is("TestCache is not ready!"));
        e = Assertions.assertThrows(RuntimeException.class, () -> cache.put("one", false));
        assertThat(e.getMessage(), is("TestCache is not ready!"));
        e = Assertions.assertThrows(RuntimeException.class, () -> cache.remove("one"));
        assertThat(e.getMessage(), is("TestCache is not ready!"));
        e = Assertions.assertThrows(RuntimeException.class, () -> cache.keys());
        assertThat(e.getMessage(), is("TestCache is not ready!"));

        // Start the cache
        cache.start();

        // Check the initial values
        assertThat(cache.get("one"), is(true));
        assertThat(cache.get("two"), is(true));
        assertThat(cache.get("three"), is(true));

        // Update the values
        cache.put("one", false);
        cache.remove("two");

        // Check the updated values
        assertThat(cache.get("one"), is(false));
        assertThat(cache.get("two"), is(nullValue()));
        assertThat(cache.get("three"), is(true));

        // Wait for cache refresh
        reload.await();

        // Check refreshed values
        assertThat(cache.get("one"), is(true));
        assertThat(cache.get("two"), is(true));
        assertThat(cache.get("three"), is(true));

        // Stop the cache
        cache.stop();

        e = Assertions.assertThrows(RuntimeException.class, () -> cache.get("one"));
        assertThat(e.getMessage(), is("TestCache is not ready!"));
        e = Assertions.assertThrows(RuntimeException.class, () -> cache.put("one", false));
        assertThat(e.getMessage(), is("TestCache is not ready!"));
        e = Assertions.assertThrows(RuntimeException.class, () -> cache.remove("one"));
        assertThat(e.getMessage(), is("TestCache is not ready!"));
        e = Assertions.assertThrows(RuntimeException.class, () -> cache.keys());
        assertThat(e.getMessage(), is("TestCache is not ready!"));
    }

    class TestCache extends AbstractCache<Boolean>  {
        public TestCache() {
            super("Test", 100L);
        }

        @Override
        protected ConcurrentHashMap<String, Boolean> loadCache() {
            ConcurrentHashMap<String, Boolean> map = new ConcurrentHashMap<>();
            map.put("one", Boolean.TRUE);
            map.put("two", Boolean.TRUE);
            map.put("three", Boolean.TRUE);

            reload.countDown();
            return map;
        }
    }
}
