/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium.page;

import io.strimzi.systemtest.selenium.SeleniumProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;

import java.time.Duration;

public class OpenshiftLoginWebPage implements IWebPage {
    private static final Logger LOGGER = LogManager.getLogger(OpenshiftLoginWebPage.class);

    SeleniumProvider selenium;

    public OpenshiftLoginWebPage(SeleniumProvider selenium) {
        this.selenium = selenium;
    }

    private WebElement getUsernameTextInput() {
        return selenium.getDriver().findElement(By.id("inputUsername"));
    }

    private WebElement getPasswordTextInput() {
        return selenium.getDriver().findElement(By.id("inputPassword"));
    }

    private WebElement getLoginButton() {
        return selenium.getDriver().findElement(By.className("btn-lg"));
    }

    private WebElement getAlertContainer() {
        return selenium.getDriver().findElement(By.className("alert"));
    }

    private WebElement getHtpasswdButton() {
        return selenium.getDriver().findElement(By.partialLinkText("htpasswd"));
    }

    public String getAlertMessage() {
        return getAlertContainer().findElement(By.className("kc-feedback-text")).getText();
    }

    private boolean checkAlert() {
        try {
            getAlertMessage();
            return false;
        } catch (Exception ignored) {
            return true;
        }
    }

    public boolean login(String username, String password) throws Exception {
        checkReachableWebPage();
        LOGGER.info("Try to login with credentials {} : {}", username, password);
        selenium.fillInputItem(getUsernameTextInput(), username);
        selenium.fillInputItem(getPasswordTextInput(), password);
        selenium.clickOnItem(getLoginButton(), "Log in");
        return checkAlert();
    }

    @Override
    public void checkReachableWebPage() {
        selenium.getDriverWait().withTimeout(Duration.ofSeconds(30)).until(ExpectedConditions.urlContains("oauth/authorize"));
        selenium.getAngularDriver().waitForAngularRequestsToFinish();
        selenium.takeScreenShot();
        selenium.clickOnItem(getHtpasswdButton(), "Htpasswd log in page");
        selenium.takeScreenShot();
    }
}
