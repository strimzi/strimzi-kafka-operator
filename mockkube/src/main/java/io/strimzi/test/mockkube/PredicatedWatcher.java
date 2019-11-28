/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Predicate;

/**
 * A Watcher guarded by a Predicate; {@link #maybeFire(HasMetadata, Watcher.Action)}
 * will call the watcher only if the predicate matches.
 * This is used to support watches on resources matching selectors, for example.
 * @param <CM> The resource type of the Watcher
 */
class PredicatedWatcher<CM extends HasMetadata> {
    private static final Logger LOGGER = LogManager.getLogger(PredicatedWatcher.class);
    private final String str;
    private final Watcher<CM> watcher;
    private final Predicate<CM> predicate;
    private final String kind;

    private PredicatedWatcher(String kind, String str, Predicate<CM> predicate, Watcher<CM> watcher) {
        this.kind = kind;
        this.str = str;
        this.watcher = watcher;
        this.predicate = predicate;
    }

    public Watcher<CM> watcher() {
        return watcher;
    }

    public Predicate<CM> predicate() {
        return predicate;
    }

    static <CM extends HasMetadata> PredicatedWatcher<CM> watcher(String kind, Watcher<CM> watcher) {
        return new PredicatedWatcher<>(kind, "watch on all", resource1 -> ((Predicate<CM>) resource -> true).test(resource1), watcher);
    }

    static <CM extends HasMetadata> PredicatedWatcher<CM> namedWatcher(String kind, String name, Watcher<CM> watcher) {
        return new PredicatedWatcher<>(kind, "watch on named " + name, resource1 -> ((Predicate<CM>) resource -> name.equals(resource.getMetadata().getName())).test(resource1), watcher);
    }

    static <CM extends HasMetadata> PredicatedWatcher<CM> predicatedWatcher(String kind, String desc, Predicate<CM> predicate, Watcher<CM> watcher) {
        return new PredicatedWatcher<>(kind, desc, resource -> predicate.test(resource), watcher);
    }

    public String toString() {
        return str + " for kind " + kind;
    }

    public void maybeFire(CM removed, Watcher.Action action) {
        if (predicate.test(removed)) {
            LOGGER.debug("Firing watcher {} with {} and resource {}", watcher, action, removed);
            watcher.eventReceived(action, removed);
        }
    }
}
