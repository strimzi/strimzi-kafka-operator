/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.rbac;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.ReadWriteUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RbacUtils {
    private static final Logger LOGGER = LogManager.getLogger(RbacUtils.class);

    /**
     * Replace all references to ClusterRole to Role.
     * This includes ClusterRoles themselves as well as RoleBindings that reference them.
     */
    public static File switchClusterRolesToRolesIfNeeded(File oldFile, boolean shouldSwitch) {
        if (shouldSwitch) {
            try {
                final String[] fileNameArr = oldFile.getName().split("-");
                // change ClusterRole to Role
                fileNameArr[1] = TestConstants.ROLE;

                final String changeFileName = Arrays.stream(fileNameArr).map(item -> "-" + item).collect(Collectors.joining()).substring(1);

                File tmpFile = Files.createTempFile(changeFileName.replace(".yaml", ""), ".yaml").toFile();
                ReadWriteUtils.writeFile(tmpFile.toPath(), ReadWriteUtils.readFile(oldFile).replace(TestConstants.CLUSTER_ROLE, TestConstants.ROLE));

                LOGGER.info("Replaced ClusterRole for Role in {}", oldFile.getAbsolutePath());

                return tmpFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return oldFile;
        }
    }
}
