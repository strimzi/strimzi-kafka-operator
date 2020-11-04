/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;

import static io.strimzi.systemtest.resources.operator.OlmResource.obtainInstallPlanName;

public class OlmUtils {

    private OlmUtils() {}

    public static void waitUntilSomeInstallPlanIsPresent() {
        TestUtils.waitFor("install plan is present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    obtainInstallPlanName();
                    return true;
                } catch (RuntimeException e)  {
                    return false;
                }
            });
    }
}
