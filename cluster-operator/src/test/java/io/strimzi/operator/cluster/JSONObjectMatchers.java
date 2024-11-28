/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JSONObjectMatchers {

    public static Matcher<JsonNode> hasSize(int size) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(JsonNode actual, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendValue(actual.size());
                if (size != actual.size()) {
                    mismatchDescription.appendText("\n There are actually ")
                            .appendValue(actual.size())
                            .appendText(" entries : ")
                            .appendValue(actual.fieldNames().toString());
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected JsonObject of size ").appendValue(size);
            }
        };
    }
    public static Matcher<JsonNode> hasKey(String key) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(JsonNode actual, Description mismatchDescription) {
                List<String> fieldNames = new ArrayList<>();
                actual.fieldNames().forEachRemaining(fieldNames::add);
                mismatchDescription.appendText("was ").appendValue(fieldNames);
                if (!fieldNames.contains(key)) {
                    mismatchDescription.appendText("\nDoes not contain desired key");
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected JsonObject with key ").appendValue(key);
            }
        };
    }

    public static Matcher<JsonNode> hasKeys(String... keys) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(JsonNode actual, Description mismatchDescription) {
                List<String> fieldNames = new ArrayList<>();
                actual.fieldNames().forEachRemaining(fieldNames::add);
                mismatchDescription.appendText("was ").appendValue(fieldNames);
                boolean matches = true;
                for (String key : keys) {
                    if (!hasKey(key).matches(actual)) {
                        mismatchDescription.appendText("\nDoes not contain key " + key);
                        matches = false;
                    }
                }

                return matches;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected JsonObject with keys ")
                        .appendValue(Arrays.toString(keys));
            }
        };
    }

    public static Matcher<JsonNode> hasEntry(String key, String value) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(JsonNode actual, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendValue(actual.get(key));
                if (!hasKey(key).matches(actual)) {
                    mismatchDescription.appendText("\nDoes not contain key " + key);
                    return false;
                }

                String actualValue = actual.get(key).asText();
                if (!value.equals(actualValue)) {
                    mismatchDescription.appendText("\nKey does not have expected value, found " + actualValue);
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected JsonObject with key ")
                        .appendValue(key)
                        .appendText(" with value ")
                        .appendValue(value);
            }
        };
    }
}