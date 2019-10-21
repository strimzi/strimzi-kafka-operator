/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.client.Watcher;

public interface Observer<T> {

    void beforeWatcherFire(Watcher.Action action, T resource);
    void afterWatcherFire(Watcher.Action action, T resource);
}
