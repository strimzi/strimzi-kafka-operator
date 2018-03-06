/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package matchers;

import org.hamcrest.Matcher;

public class Matchers {

    private Matchers() {
    }

    /**
     * Matcher to check value by key in config map
     * @param key - key of config map
     * @param value - expected value for the key
     */
    public static Matcher<String> valueOfCmEqualsTo(String key, String value) {
        return new CmMatcher(key, value);
    }
}
