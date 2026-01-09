/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.seaweedfs;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.executor.ExecResult;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class SetupSeaweedFS {
    private static final Logger LOGGER = LogManager.getLogger(SetupSeaweedFS.class);

    public static final String SEAWEEDFS = "seaweedfs";
    public static final String ADMIN_CREDS = "seaweedfsadminLongerThan16BytesForFIPS";
    public static final int SEAWEEDFS_PORT = 8333;
    private static final String SEAWEEDFS_IMAGE = "mirror.gcr.io/chrislusf/seaweedfs:3.99";

    /**
     * Deploy SeaweedFS to a specific namespace, creates service for it and init bucket
     * @param namespace where SeaweedFS will be installed to
     */
    public static void deploySeaweedFS(String namespace) {
        // Create a SeaweedFS deployment
        Deployment seaweedfsDeployment = new DeploymentBuilder()
            .withNewMetadata()
                .withName(SEAWEEDFS)
                .withNamespace(namespace)
                .withLabels(Map.of(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.SeaweedFS.name()))
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                    .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, SEAWEEDFS))
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(Map.of(TestConstants.APP_POD_LABEL, SEAWEEDFS))
                    .endMetadata()
                    .withNewSpec()
                        .addNewContainer()
                            .withName(SEAWEEDFS)
                                .withImage(SEAWEEDFS_IMAGE)
                                .withArgs("server", "-s3", "-iam", "-dir=/data", "-s3.port=" + SEAWEEDFS_PORT)
                                .addNewPort()
                                    .withContainerPort(SEAWEEDFS_PORT)
                                    .withName("s3")
                                .endPort()
                                .withNewReadinessProbe()
                                    .withNewHttpGet()
                                        .withPath("/status")
                                        .withPort(new IntOrString(SEAWEEDFS_PORT))
                                    .endHttpGet()
                                    .withInitialDelaySeconds(5)
                                    .withPeriodSeconds(5)
                                .endReadinessProbe()
                                .withVolumeMounts(new VolumeMountBuilder()
                                    .withName("seaweedfs-storage")
                                    .withMountPath("/data")
                                    .build())
                        .endContainer()
                        .withVolumes(new VolumeBuilder()
                            .withName("seaweedfs-storage")
                            .withNewEmptyDir()
                            .endEmptyDir()
                            .build())
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();

        // Create the deployment
        KubeResourceManager.get().createResourceWithWait(seaweedfsDeployment);

        // Create a service to expose SeaweedFS
        Service seaweedfsService = new ServiceBuilder()
            .withNewMetadata()
                .withName(SEAWEEDFS)
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
                .withSelector(Map.of(TestConstants.APP_POD_LABEL, SEAWEEDFS))
                .addNewPort()
                    .withName("s3")
                    .withPort(SEAWEEDFS_PORT)
                    .withTargetPort(new IntOrString(SEAWEEDFS_PORT))
                .endPort()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithoutWait(seaweedfsService);
        NetworkPolicyUtils.allowNetworkPolicyAllIngressForMatchingLabel(namespace, SEAWEEDFS, Map.of(TestConstants.APP_POD_LABEL, SEAWEEDFS));

        // Configure S3 IAM credentials
        configureS3Credentials(namespace);
    }

    /**
     * Configure S3 IAM credentials for SeaweedFS.
     * This must be called after the SeaweedFS pod is running.
     * @param namespace where SeaweedFS is installed
     */
    private static void configureS3Credentials(String namespace) {
        LabelSelector labelSelector = new LabelSelectorBuilder()
            .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, SEAWEEDFS))
            .build();

        final String seaweedfsPod = PodUtils.listPodNames(namespace, labelSelector).get(0);

        // Configure admin user with credentials
        // Based on Kubeflow approach: echo "s3.configure ..." | weed shell
        ExecResult execResult = KubeResourceManager.get().kubeCmdClient().inNamespace(namespace).execInPod(seaweedfsPod,
            "sh", "-c",
            "echo 's3.configure -user admin -access_key " + ADMIN_CREDS +
            " -secret_key " + ADMIN_CREDS + " -actions Admin,Read,Write,List -apply' | weed shell");

        LOGGER.debug(execResult.toString());
    }

    /**
     * Create bucket in SeaweedFS instance in specific namespace.
     * @param namespace SeaweedFS location
     * @param bucketName name of the bucket that will be created and used within the tests
     */
    public static void createBucket(String namespace, String bucketName) {
        LabelSelector labelSelector = new LabelSelectorBuilder()
            .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, SEAWEEDFS))
            .build();

        final String seaweedfsPod = PodUtils.listPodNames(namespace, labelSelector).get(0);

        // Create bucket using weed shell with stdin piping
        // Based on Kubeflow approach: echo "command" | weed shell
        ExecResult execResult = KubeResourceManager.get().kubeCmdClient().inNamespace(namespace).execInPod(seaweedfsPod,
            "sh", "-c",
            "echo 's3.bucket.create --name " + bucketName + "' | weed shell");

        LOGGER.debug(execResult.toString());
    }

    private SetupSeaweedFS() {
        // Private constructor to prevent instantiation
    }
}