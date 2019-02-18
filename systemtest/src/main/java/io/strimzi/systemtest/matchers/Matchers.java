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
     * A matcher checks that examined object has a full match of reasons for actual events.
     * @param eventReasons - expected events for resource
     */
    public static Matcher<List<Event>> hasAllOfReasons(Events... eventReasons) {
        return new HasAllOfReasons(eventReasons);
    }

    /**
     * A matcher checks that examined object has at least one match of reasons for actual events.
     * @param eventReasons - expected events for resource
     */
    public static Matcher<List<Event>> hasAnyOfReasons(Events... eventReasons) {
        return new HasAnyOfReasons(eventReasons);
    }

    /**
     * A matcher checks that events don't have all listed reasons
     * @param eventReasons - unexpected events for resource
     * @return a matcher {@link #hasAnyOfReasons(Events... eventReasons)} with opposite result
     */
    public static Matcher<List<Event>> hasNoneOfReasons(Events... eventReasons) {
        return new HasNoneOfReasons(eventReasons);
    }

    /**
     * A matcher checks that log doesn't have unexpected errors
     */
    public static Matcher<String> logHasNoUnexpectedErrors() {
        return new LogHasNoUnexpectedErrors();
    }
}
