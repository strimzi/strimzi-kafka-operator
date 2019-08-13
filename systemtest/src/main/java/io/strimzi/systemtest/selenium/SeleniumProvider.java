/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium;

import com.paulhammant.ngwebdriver.NgWebDriver;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.selenium.resources.WebItem;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.logging.LogEntries;
import org.openqa.selenium.logging.LogEntry;
import org.openqa.selenium.logging.LogType;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SeleniumProvider {
    private static final Logger LOGGER = LogManager.getLogger(SeleniumProvider.class);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss:SSS");
    private WebDriver driver;
    private NgWebDriver angularDriver;
    private WebDriverWait driverWait;
    private Map<Date, File> browserScreenshots = new HashMap<>();
    private String webConsoleFolder = "selenium_tests";
    private static SeleniumProvider instance;

    private SeleniumProvider() {
    }

    public static synchronized SeleniumProvider getInstance() {
        if (instance == null) {
            instance = new SeleniumProvider();
        }
        return instance;
    }

    public void onFailed(ExtensionContext extensionContext) {
        String getTestClassName = extensionContext.getTestClass().get().getName();
        String getTestMethodName = extensionContext.getTestMethod().get().getName();
        saveBrowserLog(getTestClassName, getTestMethodName);
        saveScreenShots(getTestClassName, getTestMethodName);
    }

    public void saveBrowserLog(String className, String methodName) {
        try {
            LOGGER.info("Saving browser console log...");
            Path path = Paths.get(
                    Environment.TEST_LOG_DIR,
                    webConsoleFolder,
                    className,
                    methodName,
                    StUtils.getCurrentTime());
            Files.createDirectories(path);
            File consoleLog = new File(path.toString(), "browser_console.log");
            StringBuilder logEntries = formatedBrowserLogs();
            Files.write(Paths.get(consoleLog.getPath()), logEntries.toString().getBytes());
            LOGGER.info("Browser console log saved successfully");
        } catch (Exception ex) {
            LOGGER.warn("Cannot save browser log: " + ex.getMessage());
        }
    }

    public void saveScreenShots(String className, String methodName) {
        try {
            takeScreenShot();
            Path path = Paths.get(
                    Environment.TEST_LOG_DIR,
                    webConsoleFolder,
                    className,
                    methodName);
            Files.createDirectories(path);
            for (Date key : browserScreenshots.keySet()) {
                FileUtils.copyFile(browserScreenshots.get(key), new File(Paths.get(path.toString(),
                        String.format("%s.%s_%s.png", className, methodName, dateFormat.format(key))).toString()));
            }
            LOGGER.info("Screenshots stored");
        } catch (Exception ex) {
            LOGGER.warn("Cannot save screenshots: " + ex.getMessage());
        } finally {
            tearDownDrivers();
        }
    }

    public void setupDriver(WebDriver driver) {
        this.driver = driver;
        angularDriver = new NgWebDriver((JavascriptExecutor) driver);
        driverWait = new WebDriverWait(driver, 10);
        browserScreenshots.clear();
    }


    public void tearDownDrivers() {
        LOGGER.info("Tear down selenium web drivers");
        if (driver != null) {
            try {
                driver.quit();
            } catch (Exception ex) {
                LOGGER.warn("Raise warning on quit: " + ex.getMessage());
            }
            LOGGER.info("Driver is closed");
            driver = null;
            angularDriver = null;
            driverWait = null;
            browserScreenshots.clear();
        }
    }

    private LogEntries getBrowserLog() {
        return this.driver.manage().logs().get(LogType.BROWSER);
    }

    private StringBuilder formatedBrowserLogs() {
        StringBuilder logEntries = new StringBuilder();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (LogEntry logEntry : getBrowserLog().getAll()) {
            logEntries.append(logEntry.getLevel()).append(": ")
                    .append(sdfDate.format(logEntry.getTimestamp())).append(": ")
                    .append(logEntry.getMessage()).append(System.lineSeparator());
        }
        return logEntries;
    }


    public WebDriver getDriver() {
        return this.driver;
    }

    public NgWebDriver getAngularDriver() {
        return this.angularDriver;
    }

    public WebDriverWait getDriverWait() {
        return driverWait;
    }

    public void takeScreenShot() {
        try {
            LOGGER.info("Taking screenshot");
            browserScreenshots.put(new Date(), ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE));
        } catch (Exception ex) {
            LOGGER.warn("Cannot take screenshot: {}", ex.getMessage());
        }
    }

    public void clearScreenShots() {
        if (browserScreenshots != null) {
            browserScreenshots.clear();
            LOGGER.info("Screenshots cleared");
        }
    }

    public void clickOnItem(WebElement element) {
        clickOnItem(element, null);
    }

    public void executeJavaScript(String script) {
        executeJavaScript(script, null);
    }

    public void executeJavaScript(String script, String textToLog, Object... arguments) {
        takeScreenShot();
        assertNotNull(script, "Selenium provider failed, script to execute is null");
        LOGGER.info("Execute script: " + (textToLog == null ? script : textToLog));
        ((JavascriptExecutor) driver).executeScript(script, arguments);
        angularDriver.waitForAngularRequestsToFinish();
        takeScreenShot();
    }

    public void clickOnItem(WebElement element, String textToLog) {
        takeScreenShot();
        assertNotNull(element, "Selenium provider failed, element is null");
        logCheckboxValue(element);
        LOGGER.info("Click on element: {}", textToLog == null ? element.getText() : textToLog);
        element.click();
        angularDriver.waitForAngularRequestsToFinish();
        takeScreenShot();
    }

    public void clearInput(WebElement element) {
        element.sendKeys(Keys.chord(Keys.CONTROL, "a"));
        element.sendKeys(Keys.BACK_SPACE);
        angularDriver.waitForAngularRequestsToFinish();
        LOGGER.info("Cleared input");
    }


    public void fillInputItem(WebElement element, String text) {
        takeScreenShot();
        assertNotNull(element, "Selenium provider failed, element is null");
        clearInput(element);
        element.sendKeys(text);
        angularDriver.waitForAngularRequestsToFinish();
        LOGGER.info("Filled input with text: " + text);
        takeScreenShot();
    }

    public void pressEnter(WebElement element) {
        takeScreenShot();
        assertNotNull(element, "Selenium provider failed, element is null");
        element.sendKeys(Keys.RETURN);
        angularDriver.waitForAngularRequestsToFinish();
        LOGGER.info("Enter pressed");
        takeScreenShot();
    }

    public void refreshPage() {
        takeScreenShot();
        LOGGER.info("Web page is going to be refreshed");
        driver.navigate().refresh();
        angularDriver.waitForAngularRequestsToFinish();
        LOGGER.info("Web page successfully refreshed");
        takeScreenShot();
    }

    private <T> T getElement(Supplier<T> webElement, int attempts, int count) throws Exception {
        T result = null;
        int i = 0;
        while (++i <= attempts) {
            try {
                result = webElement.get();
                if (result == null) {
                    LOGGER.warn("Element was not found, go to next iteration: {}", i);
                } else if (result instanceof WebElement) {
                    if (((WebElement) result).isEnabled()) {
                        break;
                    }
                    LOGGER.warn("Element was found, but it is not enabled, go to next iteration: {}", i);
                } else if (result instanceof List) {
                    if (((List<?>) result).size() == count) {
                        break;
                    }
                    LOGGER.warn("Elements were not found, go to next iteration: {}", i);
                }
            } catch (Exception ex) {
                LOGGER.warn("Element was not found, go to next iteration: {}", i);
            }
            Thread.sleep(1000);
        }
        return result;
    }

    public WebElement getInputByName(String inputName) {
        return this.getDriver().findElement(By.cssSelector(String.format("input[name='%s']", inputName)));
    }

    public WebElement getWebElement(Supplier<WebElement> webElement) throws Exception {
        return getElement(webElement, 30, 0);
    }

    public WebElement getWebElement(Supplier<WebElement> webElement, int attempts) throws Exception {
        return getElement(webElement, attempts, 0);
    }

    public List<WebElement> getWebElements(Supplier<List<WebElement>> webElements, int count) throws Exception {
        return getElement(webElements, 30, count);
    }

    public <T extends WebItem> T waitUntilItemPresent(int timeInSeconds, Supplier<T> item) throws Exception {
        return waitUntilItem(timeInSeconds, item, true);
    }

    public void waitUntilItemNotPresent(int timeInSeconds, Supplier<WebItem> item) throws Exception {
        waitUntilItem(timeInSeconds, item, false);
    }

    private <T extends WebItem> T waitUntilItem(int timeInSeconds, Supplier<T> item, boolean present) throws Exception {
        LOGGER.info("Waiting for element be present");
        int attempts = 0;
        T result = null;
        while (attempts++ < timeInSeconds) {
            if (present) {
                try {
                    result = item.get();
                    if (result != null) {
                        break;
                    }
                } catch (Exception ignored) {
                } finally {
                    LOGGER.info("Element not present, go to next iteration: " + attempts);
                }
            } else {
                try {
                    if (item.get() == null) {
                        break;
                    }
                } catch (Exception ignored) {
                } finally {
                    LOGGER.info("Element still present, go to next iteration: " + attempts);
                }
            }
            Thread.sleep(1000);
        }
        LOGGER.info("End of waiting");
        return result;
    }

    public void waitUntilPropertyPresent(int timeoutInSeconds, int expectedValue, Supplier<Integer> item) throws
            Exception {
        LOGGER.info("Waiting until data will be present");
        int attempts = 0;
        while (attempts < timeoutInSeconds) {
            if (expectedValue == item.get())
                break;
            Thread.sleep(1000);
            attempts++;
        }
        LOGGER.info("End of waiting");
    }

    //================================================================================================
    //==================================== Checkbox methods ==========================================
    //================================================================================================

    public void setValueOnCheckboxRequestedPermissions(WebElement element, boolean check) {
        if (getCheckboxValue(element) != check) {
            clickOnItem(element);
        } else {
            LOGGER.info("Checkbox already {}", check ? "checked" : "unchecked");
        }
    }

    public boolean getCheckboxValue(WebElement element) {
        if (isCheckbox(element)) {
            return Boolean.valueOf(element.getAttribute("checked"));
        }
        throw new IllegalStateException("Requested element is not of type 'checkbox'");
    }

    private boolean isCheckbox(WebElement element) {
        String type = element.getAttribute("type");
        return type != null && type.equals("checkbox");
    }

    private void logCheckboxValue(WebElement element) {
        if (isCheckbox(element)) {
            LOGGER.info("Checkbox value before click is checked='{}'", element.getAttribute("checked"));
        }
    }

}
