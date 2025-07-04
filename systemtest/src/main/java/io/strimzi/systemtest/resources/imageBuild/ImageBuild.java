/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.imageBuild;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildRequest;
import io.fabric8.openshift.api.model.BuildRequestBuilder;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.BuildUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.strimzi.systemtest.Environment.KANIKO_IMAGE;

public class ImageBuild {

    /**
     * Build a specific image from passed Dockerfile and push it into internal registry.
     * It will use OpenShift build on OpenShift like clusters and Kaniko on other distributions.
     *
     * @param namespace      location where the build will happen
     * @param name           Image name (it is also used as a name for all needed resources)
     * @param dockerfilePath path to Dockerfile
     * @param imageTag tag of the final image that will be pushed into internal registries
     * @param baseImage used base image for the build
     * @throws IOException  When reading from file - when file doesn't exist or cannot be opened
     */
    public static void buildImage(String namespace, String name, String dockerfilePath, String imageTag, String baseImage) throws IOException {
        if (KubeClusterResource.getInstance().isOpenShiftLikeCluster()) {
            buildImageOpenshift(namespace, name, dockerfilePath, imageTag, baseImage);
        } else {
            buildImageKaniko(namespace, name, dockerfilePath, imageTag, baseImage);
        }
    }

    /**
     * Build a specific image from passed Dockerfile and push it into internal registry.
     *
     * @param namespace      location where the build will happen
     * @param name           Image name (it is also used as a name for all needed resources)
     * @param dockerfilePath path to Dockerfile
     * @param imageTag tag of the final image that will be pushed into internal registries
     * @param baseImage used base image for the build
     * @throws IOException  When reading from file - when file doesn't exist or cannot be opened
     */
    public static void buildImageKaniko(String namespace, String name, String dockerfilePath, String imageTag, String baseImage) throws IOException {
        createDockerfileConfigMap(namespace, name, dockerfilePath);

        Job kanikoJob = new JobBuilder()
            .withNewMetadata()
                .withName(name)
                .withNamespace(namespace) // Change this to your namespace
            .endMetadata()
            .withNewSpec()
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(name)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("Never")
                        .withVolumes(
                            new VolumeBuilder()
                                    .withName(name)
                                    .withNewConfigMap()
                                        .withName(name)
                                    .endConfigMap()
                                    .build()
                        )
                        .addNewContainer()
                            .withName(name)
                            .withImage(KANIKO_IMAGE)
                            .withArgs(
                                "--dockerfile=/workspace/Dockerfile",
                                "--destination=" + Environment.getImageOutputRegistry(namespace, name, imageTag),
                                "--build-arg=BASE_IMAGE=" + baseImage,
                                "--skip-tls-verify",
                                "-v=debug")
                            .withVolumeMounts(
                                new VolumeMountBuilder()
                                        .withName(name)
                                        .withSubPath("Dockerfile")
                                        .withMountPath("/workspace/Dockerfile")
                                        .build()
                            )
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(kanikoJob);
        JobUtils.waitForJobSuccess(namespace, name, TestConstants.GLOBAL_TIMEOUT);
    }

    /**
     * Build a specific image from passed Dockerfile and push it into internal registry.
     *
     * @param namespace      location where the build will happen
     * @param name           Image name (it is also used as a name for all needed resources)
     * @param dockerfilePath path to Dockerfile
     * @param imageTag tag of the final image that will be pushed into internal registries
     * @param baseImage used base image for the build
     * @throws IOException  When reading from file - when file doesn't exist or cannot be opened
     */
    public static void buildImageOpenshift(String namespace, String name, String dockerfilePath, String imageTag, String baseImage) throws IOException {
        String dockerfileContent = Files.readString(Paths.get(dockerfilePath), StandardCharsets.UTF_8);

        BuildConfig buildConfig = new BuildConfigBuilder()
            .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
                .withNewOutput()
                    .withNewTo()
                        .withName(name + ":" + imageTag)
                        .withNamespace(namespace)
                        .withKind("ImageStreamTag")
                    .endTo()
                .endOutput()
                .withNewSource()
                    .withType("Dockerfile")
                    .withDockerfile(dockerfileContent)
                .endSource()
                .withNewStrategy()
                    .withType("Docker")
                    .withNewDockerStrategy()
                        .addToBuildArgs(new EnvVar("BASE_IMAGE", baseImage, null))
                    .endDockerStrategy()
                .endStrategy()
            .endSpec()
            .build();

        ImageStream imageStream = new ImageStreamBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .build();

        BuildRequest buildRequest = new BuildRequestBuilder()
            .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
            .endMetadata()
            .build();

        KubeResourceManager.get().createResourceWithoutWait(imageStream);
        KubeResourceManager.get().createResourceWithoutWait(buildConfig);
        KubeResourceManager.get().kubeClient().getClient().adapt(OpenShiftClient.class).buildConfigs().inNamespace(namespace).withName(name).instantiate(buildRequest);

        BuildUtils.waitForBuildComplete(namespace, name);
    }

    /**
     * Create config map with Dockerfile loaded by Kaniko
     * @param namespace location of the config map
     * @param configMapName name of the config map
     * @param dockerfilePath path to the Dockerfile
     * @throws IOException  When reading from file - when file doesn't exist or cannot be opened
     */
    private static void createDockerfileConfigMap(String namespace, String configMapName, String dockerfilePath) throws IOException {
        String dockerfileContent = Files.readString(Paths.get(dockerfilePath), StandardCharsets.UTF_8);

        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapName)
                .withNamespace(namespace)
            .endMetadata()
            .addToData("Dockerfile", dockerfileContent)
            .build();

        KubeResourceManager.get().createResourceWithWait(configMap);
    }
}
