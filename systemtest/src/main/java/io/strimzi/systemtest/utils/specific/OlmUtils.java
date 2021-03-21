/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.specific.OlmResource;
import io.strimzi.test.TestUtils;

public class OlmUtils {

    private OlmUtils() {}

    public static void waitUntilNonUsedInstallPlanIsPresent(String currentVersion) {
        TestUtils.waitFor("install plan is present in version:" + currentVersion + ".", Constants.OLM_UPGRADE_INSTALL_PLAN_POLL, Constants.OLM_UPGRADE_INSTALL_PLAN_TIMEOUT,
            () -> {
                try {
                    OlmResource.obtainInstallPlanName();
                    return !OlmResource.getNonUsedInstallPlan().equals(OlmResource.NO_MORE_NON_USED_INSTALL_PLANS);
                } catch (RuntimeException e)  {
                    throw new RuntimeException("No install-plan was found upgrading from:"  + currentVersion + " version! It must exists.");
                }
            });
    }
}
