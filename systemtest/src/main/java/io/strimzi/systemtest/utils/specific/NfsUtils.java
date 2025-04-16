/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Predicate;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NfsUtils {
    private static final Logger LOGGER = LogManager.getLogger(NfsUtils.class);
    private static final String NFS_POD_NAME_PREFIX = "test-nfs-server-provisioner";
    public static final String NFS_PVC_NAME = "nfs-pvc";

    private NfsUtils() {
    }

    private static String getNfsVolumeName(String namespaceName) {
        return kubeClient().getPersistentVolumeClaim(namespaceName, NFS_PVC_NAME).getSpec().getVolumeName();
    }

    private static String getNfsPodName(String namespaceName) {
        return kubeClient().listPodsByPrefixInName(namespaceName, NFS_POD_NAME_PREFIX).get(0).getMetadata().getName();
    }

    /**
     * Get size in bytes for the path in the pod
     *
     * @param namespaceName the namespace of NFS location
     * @param podName       the pod name to exec
     * @param path          the path to check size
     */
    private static String getSizeOfDirectoryInPod(final String namespaceName, String podName, String path) {
        return cmdKubeClient().namespace(namespaceName).execInPod(podName, "du", "-sb", path).out();
    }

    /**
     * Wait until size of the NFS meets the <code>sizePredicate</code>.
     *
     * @param namespaceName the namespace of NFS location
     * @param sizePredicate predicate to test the size in the nfs folder
     */
    public static void waitForSizeInNfs(String namespaceName, Predicate<Long> sizePredicate) {
        TestUtils.waitFor("data in NFS meet the size predicate.", TestConstants.GLOBAL_POLL_INTERVAL_5_SECS, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            String output = getSizeOfDirectoryInPod(namespaceName, getNfsPodName(namespaceName), "/export/" + getNfsVolumeName(namespaceName));
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
