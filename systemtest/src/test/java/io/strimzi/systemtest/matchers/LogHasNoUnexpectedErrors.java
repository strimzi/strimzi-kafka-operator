/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * <p>A LogHasNoUnexpectedErrors is custom matcher to check log form kubernetes client
 * doesn't have any unexpected errors. </p>
 */
public class LogHasNoUnexpectedErrors extends BaseMatcher<String> {

    @Override
    public boolean matches(Object actualValue) {
        if (!"".equals(actualValue)) {
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
        // "NO_ERROR" is necessary because DnsNameResolver prints debug information `QUERY(0), NoError(0), RD RA` after `recived` operation
        NO_ERROR("NoError(0)");

        final String name;

        LogWhiteList(String name) {
            this.name = name;
        }
    }
}
