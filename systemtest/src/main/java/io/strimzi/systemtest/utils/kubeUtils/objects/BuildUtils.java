/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.openshift.api.model.BuildStatus;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.openshift.BuildConfigResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BuildUtils {

    private static final Logger LOGGER = LogManager.getLogger(BuildUtils.class);

    /**
     * Gets OpenShift build name based on name and version
     * @param name
     * @param version
     * @return
     */
    public static String getBuildName(String name, Long version) {
        return name + "-" + version;
    }

    /**
     * Waits for build status to be complete which indicates successful build.
     * @param buildConfigName name of the corresponding BuildConfig resource
     * @param namespace namespace where the resources are deployed
     */
    public static void waitForBuildComplete(String buildConfigName, String namespace) {
        TestUtils.waitFor("build " + buildConfigName + " complete", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT_SHORT, () -> {
            Long buildLatestVersion = BuildConfigResource.buildConfigClient().inNamespace(namespace).withName(buildConfigName).get().getStatus().getLastVersion();
            String buildName = getBuildName(buildConfigName, buildLatestVersion);

            BuildStatus buildStatus = BuildConfigResource.buildsClient().inNamespace(namespace).withName(buildName).get().getStatus();

            LOGGER.info("Build status of {} is '{}'", buildName, buildStatus.getPhase());

            return buildStatus.getPhase().equals("Complete");
        });
    }
}
