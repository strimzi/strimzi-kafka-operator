/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium.page;

import io.strimzi.systemtest.selenium.SeleniumProvider;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

public interface IWebPage {

    /**
     * Method for check if current opened web page is AuthorizeAccessWebPage
     * this method should be called in constructor
     */
    void checkReachableWebPage();

    default void checkTitle(SeleniumProvider selenium, String webPageTitle) {
        boolean isOpened = false;
        try {
            WebElement title = selenium.getDriver().findElement(By.tagName("title"));
            if (title != null && title.getText() != null) {
                //format of <title> innerText because it's insane as default
                String titleContent = title.getAttribute("innerText")
                        .replaceAll(System.lineSeparator(), " ")
                        .replaceAll(" +", " ");
                isOpened = titleContent.contains(webPageTitle);
            }
        } catch (Exception ignored) {
        }
        if (!isOpened) {
            selenium.takeScreenShot();
            throw new IllegalStateException("Unexpected web page in browser!");
        }
    }

}
