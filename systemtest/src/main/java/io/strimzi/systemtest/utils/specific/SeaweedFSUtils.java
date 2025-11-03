/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.seaweedfs.SetupSeaweedFS;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SeaweedFSUtils {
    private static final Logger LOGGER = LogManager.getLogger(SeaweedFSUtils.class);

    private SeaweedFSUtils() {

    }

    /**
     * Get total size of all objects in a bucket using weed shell
     * @param namespaceName Name of the Namespace where the SeaweedFS Pod is running
     * @param bucketName Name of the bucket for which we want to get info about its size
     * @return Total size in bytes
     */
    public static long getBucketSize(String namespaceName, String bucketName) {
        LabelSelector labelSelector = new LabelSelectorBuilder()
            .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, SetupSeaweedFS.SEAWEEDFS))
            .build();

        final String seaweedfsPod = KubeResourceManager.get().kubeClient().listPods(namespaceName, labelSelector).get(0).getMetadata().getName();

        // Use weed shell with stdin piping to list bucket info
        // Based on Kubeflow approach: echo "command" | weed shell
        String output = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(seaweedfsPod,
            "sh", "-c",
            "echo 's3.bucket.list' | weed shell | grep '" + bucketName + "'"
        ).out();

        return parseBucketSize(output);
    }

    /**
     * Parse the bucket chunk count from weed shell output
     * @param output Output from weed shell command
     * @return Chunk count (0 means empty, >0 means has data)
     */
    private static long parseBucketSize(String output) {
        // Output is in format 'test-bucket	size:306688	chunk:0' where
        // chunks represents number of files in the bucket (excluding deleted files)
        // Size represents total collection size in bytes with also metedata, indexes, etc. so it won't be 0
        Pattern pattern = Pattern.compile("chunk:(\\d+)");
        Matcher matcher = pattern.matcher(output);

        if (matcher.find()) {
            long chunkCount = Long.parseLong(matcher.group(1));
            LOGGER.debug("Bucket has {} chunks", chunkCount);
            return chunkCount;
        }

        // If we can't parse, assume empty
        LOGGER.debug("Could not parse chunk count, assuming bucket is empty");
        return 0;
    }

    /**
     * Wait until the bucket contains data (object count > 0).
     *
     * @param namespaceName SeaweedFS location
     * @param bucketName bucket name
     */
    public static void waitForDataInSeaweedFS(String namespaceName, String bucketName) {
        TestUtils.waitFor("data sync from Kafka to SeaweedFS", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            long objectCount = getBucketSize(namespaceName, bucketName);
            LOGGER.info("Bucket '{}' contains {} objects", bucketName, objectCount);

            return objectCount > 0;
        });
    }

    /**
     * Wait until the bucket is empty (object count == 0).
     *
     * @param namespaceName SeaweedFS location
     * @param bucketName bucket name
     */
    public static void waitForNoDataInSeaweedFS(String namespaceName, String bucketName) {
        TestUtils.waitFor("data deletion in SeaweedFS", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            long objectCount = getBucketSize(namespaceName, bucketName);
            LOGGER.info("Bucket '{}' contains {} chunks", bucketName, objectCount);

            return objectCount == 0;
        });
    }
}