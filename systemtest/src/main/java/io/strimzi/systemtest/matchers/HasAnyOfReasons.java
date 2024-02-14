/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.systemtest.k8s.Events;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Arrays;
import java.util.List;

/**
 * <p>A HasAllOfReasons is custom matcher to check the partial matching of reasons for actual events.
 * Checks at least one match in events.</p>
 */
public class HasAnyOfReasons extends BaseMatcher<List<Event>> {

    private Events[] eventReasons;

    public HasAnyOfReasons(Events... eventReasons) {
        this.eventReasons = eventReasons;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean matches(Object actualValue) {
        List<String> actualReasons = ((List<Event>) actualValue).stream()
                .map(Event::getReason)
                .toList();

        List<String> expectedReasons = Arrays.stream(eventReasons)
                .map(Enum::name)
                .toList();

        for (String actualReason : actualReasons) {
            if (expectedReasons.contains(actualReason)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void describeTo(Description description) {
        description.appendValueList("The resource should contain at least one the following events {", ", ", "}. ", eventReasons);
    }
}
