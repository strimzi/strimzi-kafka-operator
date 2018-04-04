/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.systemtest.k8s.Events;
import org.hamcrest.Matcher;

import java.util.List;

public class Matchers {

    private Matchers() {
    }

    /**
     * Matcher to check value by key in config map
     * @param key - key of config map
     * @param value - expected value for the key
     */
    public static Matcher<String> valueOfCmEquals(String key, String value) {
        return new CmMatcher(key, value);
    }

    /**
     * Matcher to check events for resource
     * @param eventReasons - expected events for resource
     */
    public static Matcher<List<Event>> hasReasons(Events... eventReasons) {
        return new EventMatcher(eventReasons);
    }
}
