/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.strimzi.systemtest.Resources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SeleniumManagement {
    private static final Logger LOGGER = LogManager.getLogger(SeleniumManagement.class);

    public static void deployFirefoxApp() throws Exception {
        deployFirefoxApp(false);
    }

    public static void deployFirefoxApp(Boolean isFullClass) throws Exception {
        String operationID = TimeMeasuringSystem.startOperation(Operation.CREATE_SELENIUM);
        LOGGER.info("Deploy firefox deployment");
        try {
            Resources.deployFirefoxSeleniumApp(Resources.SELENIUM_PROJECT, KubeClusterResource.getKubeClusterResource().client(), isFullClass);
        } catch (Exception e) {
            LOGGER.error("Deployment of firefox app failed", e);
            throw e;
        } finally {
            TimeMeasuringSystem.stopOperation(operationID);
        }
    }

    public static void removeFirefoxApp() throws Exception {
        String operationID = TimeMeasuringSystem.startOperation(Operation.DELETE_SELENIUM);
        Resources.deleteFirefoxSeleniumApp(Resources.SELENIUM_PROJECT, KubeClusterResource.getKubeClusterResource().client());
        TimeMeasuringSystem.stopOperation(operationID);
    }

    public static void restartSeleniumApp() {
        LabelSelector seleniumPodSelector = new LabelSelectorBuilder().addToMatchLabels("app", Resources.SELENIUM_FIREFOX).build();
        int attempts = 5;
        for (int i = 1; i <= attempts; i++) {
            Resources.deleteSeleniumPod(Resources.SELENIUM_PROJECT, KubeClusterResource.getKubeClusterResource().client());
            try {
                StUtils.waitForPodsReady(seleniumPodSelector, 1, true, Resources.SELENIUM_PROJECT);
                break;
            } catch (Exception ex) {
                LOGGER.warn("Selenium application was not redeployed correctly, try it again {}/{}", i, attempts);
            }
        }
    }
}
