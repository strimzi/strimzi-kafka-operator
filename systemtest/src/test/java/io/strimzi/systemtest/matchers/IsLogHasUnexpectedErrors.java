/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * <p>A IsLogHasUnexpectedErrors is custom matcher to check log form kubernetes client
 * doesn't have any unexpected errors. </p>
 */
public class IsLogHasUnexpectedErrors extends BaseMatcher<String> {

    @Override
    public boolean matches(Object actualValue) {
        if (actualValue != null && actualValue instanceof String && !actualValue.equals("")) {
            for (LogWhiteList value : LogWhiteList.values()) {
                if (((String) actualValue).contains(value.name)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("The log should not contain unexpected errors.");
    }

    enum LogWhiteList {
        CO_TIMEOUT_EXCEPTION("io.strimzi.operator.cluster.operator.resource.TimeoutException"),
        VERTEX_EXCEPTION("io.vertx.core.VertxException: Thread blocked"),
        NO_ERROR("NoError");

        final String name;

        LogWhiteList(String name) {
            this.name = name;
        }
    }
}
