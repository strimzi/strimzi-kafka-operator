/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;


import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasEntry;

public class CustomMatchers {

    private CustomMatchers() { }

    /**
     * hasEntries is a custom matcher that checks that the entries in the provided map
     * are contained within the provided actual map
     *
     * @param entries a map of entries expected in the actual map
     * @return a custom Matcher which iterates through entries and delegates matching to hasEntry
     */
    public static Matcher<Map<String, String>> hasEntries(Map<String, String> entries) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected Map with entries").appendValue(entries);
            }

            @Override
            protected boolean matchesSafely(Map<String, String> actual, Description mismatchDescription) {
                Map<String, String> misMatchedEntries = entries.entrySet()
                        .stream()
                        .filter(entry -> !hasEntry(entry.getKey(), entry.getValue()).matches(actual))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                mismatchDescription.appendText(" was ").appendValue(actual)
                        .appendText("\nMismatched entries : ").appendValue(misMatchedEntries);
                return misMatchedEntries.isEmpty();
            }
        };
    }
}