/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Predicate;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NfsUtils {
    private static final Logger LOGGER = LogManager.getLogger(NfsUtils.class);
    private static final String PVC_NAME = "nfs-pvc";
    private static final String NFS_POD_NAME_PREFIX = "test-nfs-server-provisioner";

    private NfsUtils() {
    }

    private static String getNfsVolumeName(String namespaceName) {
        return kubeClient().getPersistentVolumeClaim(namespaceName, PVC_NAME).getSpec().getVolumeName();
    }

    private static String getNfsPodName(String namespaceName) {
        return kubeClient().listPodsByPrefixInName(namespaceName, NFS_POD_NAME_PREFIX).get(0).getMetadata().getName();
    }

    /**
     * Wait until size of the NFS meets the <code>sizePredicate</code>.
     *
     * @param namespaceName the namespace of NFS location
     * @param sizePredicate predicate to test the size in the nfs folder
     */
    public static void waitForSizeInNfs(String namespaceName, Predicate<Long> sizePredicate) {
        // wait for remote data deletion
        TestUtils.waitFor("data deletion in remote folder", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            String output = KafkaCmdClient.getSizeOfDirectory(namespaceName, getNfsPodName(namespaceName), "/export/" + getNfsVolumeName(namespaceName));
            String[] parsed = output.split("\\s+");
            if (parsed.length != 2) {
                return false;
            }
            long sizeInByte = Long.parseLong(parsed[0]);
            LOGGER.info("Collected NFS folder size: {} bytes", sizeInByte);
            return sizePredicate.test(sizeInByte);
        });
    }
}
