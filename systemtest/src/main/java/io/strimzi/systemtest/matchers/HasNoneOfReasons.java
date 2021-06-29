/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.systemtest.k8s.Events;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

/**
 * <p>A HasAllOfReasons is custom matcher to check the partial matching of reasons for actual events.
 * Checks at least one match in events.</p>
 */
public class HasNoneOfReasons extends BaseMatcher<List<Event>> {

    private final Set<String> prohibitedEvents;

    public HasNoneOfReasons(Events... eventReasons) {
        prohibitedEvents = stream(eventReasons)
                .map(Enum::name)
                .collect(Collectors.toSet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean matches(Object actualValue) {
        return filtered((List<Event>) actualValue).findFirst().isEmpty();
    }

    private Stream<Event> filtered(List<Event> actualValue) {
        return actualValue.stream().filter(evt -> prohibitedEvents.contains(evt.getReason()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void describeMismatch(Object item, Description description) {
        describeTo(description);
        description.appendValueList(" but actual event reasons were {", ", ", "}.",
                ((List<Event>) item).stream().map(evt -> {
                    String objRef = "";
                    if (evt.getInvolvedObject() != null) {
                        objRef = " involved object: " + evt.getInvolvedObject().getKind() + "/" + evt.getInvolvedObject().getName();
                    }
                    return evt.getReason() + " (" + evt.getType() + " " + evt.getMessage() + objRef + ")\n";
                })
                        .collect(Collectors.toList()));
    }

    /**
     * Generates a description of the object.  The description may be part of a
     * a description of a larger object of which this is just a component, so it
     * should be worded appropriately.
     *
     * @param description The description to be built or appended to.
     */
    @Override
    public void describeTo(Description description) {
        description.appendValueList("The resource should not contain no events with any of the reasons {", ", ", "}, ",
                prohibitedEvents);
    }
}
