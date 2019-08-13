/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium;

import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.utils.StUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class SeleniumFirefoxExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback, BeforeAllCallback, AfterAllCallback {
    private boolean isFullClass = false;

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        isFullClass = false;
        SeleniumProvider.getInstance().tearDownDrivers();
        SeleniumManagement.removeFirefoxApp();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        isFullClass = true;
        SeleniumManagement.deployFirefoxApp(isFullClass);
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) throws Exception {
        if (extensionContext.getExecutionException().isPresent() || Environment.storeScreenshots()) {
            SeleniumProvider.getInstance().onFailed(extensionContext);
        }
        SeleniumProvider.getInstance().tearDownDrivers();
        if (!isFullClass) {
            SeleniumManagement.removeFirefoxApp();
        } else {
            SeleniumManagement.restartSeleniumApp();
        }
    }

    @Override
    public void beforeTestExecution(ExtensionContext extensionContext) throws Exception {
        if (!isFullClass) {
            SeleniumManagement.deployFirefoxApp();
        }
        if (SeleniumProvider.getInstance().getDriver() == null)
            SeleniumProvider.getInstance().setupDriver(StUtils.getFirefoxDriver());
        else
            SeleniumProvider.getInstance().clearScreenShots();
    }
}
