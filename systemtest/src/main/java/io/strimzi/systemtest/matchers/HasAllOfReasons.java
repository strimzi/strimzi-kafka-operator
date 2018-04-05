/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.systemtest.k8s.Events;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>A HasAllOfReasons is custom matcher to check the full matching of reasons for actual events.</p>
 */
public class HasAllOfReasons extends BaseMatcher<List<Event>> {

    private Events[] eventReasons;

    public HasAllOfReasons(Events... eventReasons) {
        this.eventReasons = eventReasons;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean matches(Object actualValue) {
        List<String> actualReasons = ((List<Event>) actualValue).stream()
                .map(Event::getReason)
                .collect(Collectors.toList());

        List<String> expectedReasons = Arrays.stream(eventReasons)
                .map(Enum::name)
                .collect(Collectors.toList());

        return actualReasons.containsAll(expectedReasons);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValueList("The resource should contain all of the following events {", ", ", "}. ", eventReasons);
    }
}
