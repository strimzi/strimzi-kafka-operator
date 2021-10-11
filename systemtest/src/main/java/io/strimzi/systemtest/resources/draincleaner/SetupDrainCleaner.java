/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.draincleaner;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;

public class SetupDrainCleaner {

    public static final String PATH_TO_DC_CONFIG = TestUtils.USER_PATH + "/../packaging/install/drain-cleaner/kubernetes";

    private static final Logger LOGGER = LogManager.getLogger(SetupDrainCleaner.class);
    private static Stack<String> createdFiles = new Stack<>();

    public void applyInstallFiles() {
        List<File> drainCleanerFiles = Arrays.stream(new File(PATH_TO_DC_CONFIG).listFiles()).sorted()
            .filter(File::isFile)
            .collect(Collectors.toList());

        drainCleanerFiles.forEach(file -> {
            if (!file.getName().contains("README") && !file.getName().contains("Namespace")) {
                LOGGER.info(String.format("Applying: %s", file.getAbsolutePath()));
                cmdKubeClient().namespace(Constants.DRAIN_CLEANER_NAMESPACE).apply(file);
                createdFiles.add(file.getAbsolutePath());
            }
        });
    }

    public void deleteInstallFiles() {
        while (!createdFiles.empty()) {
            String fileToBeDeleted = createdFiles.pop();
            LOGGER.info("Deleting: {}", fileToBeDeleted);
            cmdKubeClient().namespace(Constants.DRAIN_CLEANER_NAMESPACE).delete(fileToBeDeleted);
        }
    }

    public void createDrainCleaner(ExtensionContext extensionContext) {
        applyInstallFiles();
        ResourceManager.getInstance().createResource(extensionContext, new DrainCleanerResource().buildDrainCleanerDeployment().build());
    }

    public void teardownDrainCleaner() {
        deleteInstallFiles();
    }
}
