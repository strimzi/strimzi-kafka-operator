/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.minio;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SetupMinio {
    private static final Logger LOGGER = LogManager.getLogger(SetupMinio.class);

    public static final String MINIO = "minio";
    public static final String ADMIN_CREDS = "minioadmin";
    public static final String MINIO_STORAGE_ALIAS = "local";
    public static final int MINIO_PORT = 9000;
    private static final String MINIO_IMAGE = "quay.io/minio/minio:latest";

    /**
     * Deploy minio to a specific namespace, creates service for it and init client inside the Minio pod
     * @param namespace where Minio will be installed to
     */
    public static void deployMinio(String namespace) {
        // Create a Minio deployment
        Deployment minioDeployment = new DeploymentBuilder()
            .withNewMetadata()
                .withName(MINIO)
                .withNamespace(namespace)
                .withLabels(Map.of(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.Minio.name()))
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                    .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, MINIO))
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(Map.of(TestConstants.APP_POD_LABEL, MINIO))
                    .endMetadata()
                    .withNewSpec()
                        .addNewContainer()
                            .withName(MINIO)
                                .withImage(MINIO_IMAGE)
                                .withArgs("server", "/data")
                                .addToEnv(new EnvVar("MINIO_ACCESS_KEY", ADMIN_CREDS, null))
                                .addToEnv(new EnvVar("MINIO_SECRET_KEY", ADMIN_CREDS, null))
                                .addNewPort()
                                    .withContainerPort(MINIO_PORT)
                                .endPort()
                                .withVolumeMounts(new VolumeMountBuilder()
                                    .withName("minio-storage")
                                    .withMountPath("/data")
                                    .build())
                        .endContainer()
                        .withVolumes(new VolumeBuilder()
                            .withName("minio-storage")
                            .withNewEmptyDir()
                            .endEmptyDir()
                            .build())
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();

        // Create the deployment
        ResourceManager.getInstance().createResourceWithWait(minioDeployment);

        // Create a service to expose Minio
        Service minioService = new ServiceBuilder()
            .withNewMetadata()
                .withName(MINIO)
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
                .withSelector(Map.of(TestConstants.APP_POD_LABEL, MINIO))
                .addNewPort()
                    .withPort(MINIO_PORT)
                    .withTargetPort(new IntOrString(MINIO_PORT))
                .endPort()
            .endSpec()
            .build();

        ResourceManager.getInstance().createResourceWithoutWait(minioService);
        NetworkPolicyResource.allowNetworkPolicyAllIngressForMatchingLabel(namespace, MINIO, Map.of(TestConstants.APP_POD_LABEL, MINIO));

        initMinioClient(namespace);
    }

    /**
     * Init client inside the Minio pod. This allows other commands to be executed during the tests.
     * @param namespace where Minio is installed
     */
    private static void initMinioClient(String namespace) {
        final String minioPod = ResourceManager.kubeClient().listPods(namespace, Map.of(TestConstants.APP_POD_LABEL, MINIO)).get(0).getMetadata().getName();

        ResourceManager.cmdKubeClient().namespace(namespace).execInPod(minioPod,
            "mc",
            "config",
            "host",
            "add",
            MINIO_STORAGE_ALIAS,
            "http://localhost:" + MINIO_PORT,
            ADMIN_CREDS, ADMIN_CREDS);
    }

    /**
     * Create bucket in Minio instance in specific namespace.
     * @param namespace Minio location
     * @param bucketName name of the bucket that will be created and used within the tests
     */
    public static void createBucket(String namespace, String bucketName) {
        final String minioPod = ResourceManager.kubeClient().listPods(namespace, Map.of(TestConstants.APP_POD_LABEL, MINIO)).get(0).getMetadata().getName();

        ResourceManager.cmdKubeClient().namespace(namespace).execInPod(minioPod,
            "mc",
            "mb",
            MINIO_STORAGE_ALIAS + "/" + bucketName);
    }

    /**
     * Collect data from Minio about usage of a specific bucket
     * @param namespace
     * @param bucketName
     * @return Overall statistics about the bucket in String format
     */
    public static String getBucketSizeInfo(String namespace, String bucketName) {
        final String minioPod = ResourceManager.kubeClient().listPods(namespace, Map.of(TestConstants.APP_POD_LABEL, MINIO)).get(0).getMetadata().getName();

        return ResourceManager.cmdKubeClient().namespace(namespace).execInPod(minioPod,
            "mc",
            "stat",
            "local/" + bucketName).out();

    }

    /**
     * Parse out total size of bucket from the information about usage.
     * @param bucketInfo String containing all stat info about bucket
     * @return Map consists of parsed size and it's unit
     */
    private static Map<String, Object> parseTotalSize(String bucketInfo) {
        Pattern pattern = Pattern.compile("Total size:\\s*(?<size>[\\d.]+)\\s*(?<unit>.*)");
        Matcher matcher = pattern.matcher(bucketInfo);

        if (matcher.find()) {
            return Map.of("size", Double.parseDouble(matcher.group("size")), "unit", matcher.group("unit"));
        } else {
            throw new IllegalArgumentException("Total size not found in the provided string");
        }
    }

    /**
     * Wait until size of the bucket is not 0 B.
     * @param namespace Minio location
     * @param bucketName bucket name
     */
    public static void waitForDataInMinio(String namespace, String bucketName) {
        TestUtils.waitFor("data sync from Kafka to Minio", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            String bucketSizeInfo = SetupMinio.getBucketSizeInfo(namespace, bucketName);
            Map<String, Object> parsedSize = parseTotalSize(bucketSizeInfo);
            double bucketSize = (Double) parsedSize.get("size");
            LOGGER.info("Collected bucket size: {} {}", bucketSize, parsedSize.get("unit"));
            LOGGER.debug("Collected bucket info:\n{}", bucketSizeInfo);

            return bucketSize > 0;
        });
    }
}
