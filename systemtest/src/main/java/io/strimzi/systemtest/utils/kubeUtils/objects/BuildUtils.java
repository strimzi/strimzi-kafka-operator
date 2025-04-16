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

    private BuildUtils() { }

    /**
     * Gets OpenShift build name based on name and version
     * @param name      Name of the build
     * @param version   Version of the build
     * @return  Returns `name-version` build name
     */
    public static String getBuildName(String name, Long version) {
        return name + "-" + version;
    }

    /**
     * Waits for build status to be complete which indicates successful build.
     *
     * @param namespaceName   namespace where the resources are deployed
     * @param buildConfigName name of the corresponding BuildConfig resource
     */
    public static void waitForBuildComplete(String namespaceName, String buildConfigName) {
        LOGGER.info("Waiting for build of {} to be completed", buildConfigName);

        TestUtils.waitFor("build " + buildConfigName + " complete", TestConstants.GLOBAL_POLL_INTERVAL_5_SECS, TestConstants.GLOBAL_TIMEOUT, () -> {
            Long buildLatestVersion = BuildConfigResource.buildConfigClient().inNamespace(namespaceName).withName(buildConfigName).get().getStatus().getLastVersion();
            String buildName = getBuildName(buildConfigName, buildLatestVersion);

            BuildStatus buildStatus = BuildConfigResource.buildsClient().inNamespace(namespaceName).withName(buildName).get().getStatus();

            LOGGER.debug("Build status of {} is '{}'", buildName, buildStatus.getPhase());

            return buildStatus.getPhase().equals("Complete");
        });
    }
}
