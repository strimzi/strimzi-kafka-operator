/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.api.model.Build;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.Output;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectBuildUtils;
import io.strimzi.operator.cluster.model.KafkaConnectDockerfile;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the Kafka Connect Build
 */
public class ConnectBuildOperator {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ConnectBuildOperator.class.getName());

    private final DeploymentOperator deploymentOperations;
    private final ImageStreamOperator imageStreamOperations;
    private final PodOperator podOperator;
    private final ConfigMapOperator configMapOperations;
    private final ServiceAccountOperator serviceAccountOperations;
    private final BuildConfigOperator buildConfigOperator;
    private final BuildOperator buildOperator;

    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;
    private final long connectBuildTimeoutMs;
    private final PlatformFeaturesAvailability pfa;

    /**
     * Constructor
     *
     * @param pfa       Describes the features available in the Kubernetes cluster
     * @param supplier  Resource operator supplier
     * @param config    Cluster OPerator configuration
     */
    public ConnectBuildOperator(PlatformFeaturesAvailability pfa, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
        this.deploymentOperations = supplier.deploymentOperations;
        this.imageStreamOperations = supplier.imageStreamOperations;
        this.podOperator = supplier.podOperations;
        this.configMapOperations = supplier.configMapOperations;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.buildConfigOperator = supplier.buildConfigOperations;
        this.buildOperator = supplier.buildOperations;

        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.connectBuildTimeoutMs = config.getConnectBuildTimeoutMs();
        this.pfa = pfa;
    }

    /**
     * Asynchronously runs the KafkaConnectBuild (if present) and reconciles the related resources
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace of the Connect cluster
     * @param connectName       Name of the Connect cluster
     * @param connectBuild      KafkaConnectBuild object
     * @return                  Future for tracking the asynchronous result of the reconciliation steps
     */
    public Future<BuildInfo> reconcile(Reconciliation reconciliation, String namespace, String connectName, KafkaConnectBuild connectBuild) {
        if (connectBuild.getBuild() == null) {
            // Build is not configured => we should delete resources
            return configMapOperations.reconcile(reconciliation, namespace, KafkaConnectResources.dockerFileConfigMapName(connectBuild.getCluster()), null)
                    .compose(ignore -> podOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), null))
                    .compose(ignore -> serviceAccountOperations.reconcile(reconciliation, namespace, KafkaConnectResources.buildServiceAccountName(connectBuild.getCluster()), null))
                    .compose(ignore -> pfa.supportsS2I() ? buildConfigOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), null) : Future.succeededFuture())
                    .map(i -> null);
        } else {
            // Build exists => let's build
            return deploymentOperations.getAsync(namespace, connectName)
                    .compose(deployment -> build(reconciliation, namespace, connectBuild, deployment));

        }
    }

    /**
     * Builds a new container image with connectors on Kubernetes using Kaniko or on OpenShift using BuildConfig
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace of the Connect cluster
     * @param connectBuild      KafkaConnectBuild object
     * @param deployment    The existing Connect deployment
     *
     * @return              Future for tracking the asynchronous result of the Kubernetes image build
     */
    private Future<BuildInfo> build(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, Deployment deployment) {
        String currentBuildRevision = "";
        String currentImage = "";
        boolean forceRebuild = false;
        if (deployment != null) {
            // Extract information from the current deployment. This is used to figure out if new build needs to be run or not.
            currentBuildRevision = Annotations.stringAnnotation(deployment.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null);
            currentImage = deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();
            forceRebuild = Annotations.hasAnnotation(deployment, Annotations.STRIMZI_IO_CONNECT_FORCE_REBUILD);
        }

        KafkaConnectDockerfile dockerfile = connectBuild.generateDockerfile();
        String newBuildRevision = dockerfile.hashStub() + Util.hashStub(connectBuild.getBuild().getOutput().getImage());
        ConfigMap dockerFileConfigMap = connectBuild.generateDockerfileConfigMap(dockerfile);

        if (newBuildRevision.equals(currentBuildRevision)
                && !forceRebuild) {
            // The revision is the same and rebuild was not forced => nothing to do
            LOGGER.debugCr(reconciliation, "Build configuration did not change. Nothing new to build. Container image {} will be used.", currentImage);
            return Future.succeededFuture(new BuildInfo(currentImage, newBuildRevision));
        } else if (pfa.supportsS2I()) {
            // Revisions differ, and we have S2I support => we are on OpenShift and should do a build
            return openShiftBuild(reconciliation, namespace, connectBuild, forceRebuild, dockerfile, newBuildRevision)
                    .compose(image -> Future.succeededFuture(new BuildInfo(image, newBuildRevision)));
        } else {
            // Revisions differ, and no S2I support => we are on Kubernetes and should do a build
            return kubernetesBuild(reconciliation, namespace, connectBuild, forceRebuild, dockerFileConfigMap, newBuildRevision)
                    .compose(image -> Future.succeededFuture(new BuildInfo(image, newBuildRevision)));
        }
    }

    /**
     * Executes the Kafka Connect Build on Kubernetes. Run only if needed because of changes to the Dockerfile or when
     * triggered by annotation.
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace of the Connect cluster
     * @param connectBuild          KafkaConnectBuild object
     * @param forceRebuild          If true, force a new build even if one is already in progress
     * @param dockerFileConfigMap   ConfigMap with the generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    private Future<String> kubernetesBuild(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, boolean forceRebuild, ConfigMap dockerFileConfigMap, String newBuildRevision)  {
        final AtomicReference<String> buildImage = new AtomicReference<>();
        String buildPodName = KafkaConnectResources.buildPodName(connectBuild.getCluster());

        return podOperator.getAsync(namespace, buildPodName)
                .compose(pod -> {
                    if (pod != null)    {
                        String existingBuildRevision = Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null);
                        if (newBuildRevision.equals(existingBuildRevision)
                                && !KafkaConnectBuildUtils.buildPodFailed(pod, KafkaConnectBuildUtils.getBuildContainerName(connectBuild.getCluster(), pfa.isOpenshift()))
                                && !forceRebuild) {
                            // Builder pod exists, is not failed, and is building the same Dockerfile, and we are not
                            // asked to force re-build by the annotation => we re-use the existing build
                            LOGGER.infoCr(reconciliation, "Previous build exists with the same Dockerfile and will be reused.");
                            return Future.succeededFuture();
                        } else {
                            // Pod exists, but it either failed or is for different Dockerfile => start new build
                            LOGGER.infoCr(reconciliation, "Previous build exists, but uses different Dockerfile or failed. New build will be started.");
                            return podOperator.reconcile(reconciliation, namespace, buildPodName, null)
                                    .compose(ignore -> kubernetesBuildStart(reconciliation, namespace, connectBuild, dockerFileConfigMap, newBuildRevision));
                        }
                    } else {
                        // Pod does not exist => Start new build
                        return kubernetesBuildStart(reconciliation, namespace, connectBuild, dockerFileConfigMap, newBuildRevision);
                    }
                })
                .compose(ignore -> kubernetesBuildWaitForFinish(reconciliation, namespace, connectBuild))
                .compose(image -> {
                    buildImage.set(image);
                    return podOperator.reconcile(reconciliation, namespace, buildPodName, null);
                })
                .compose(ignore -> pfa.supportsS2I() ? buildConfigOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), null) : Future.succeededFuture())
                .map(ignore -> buildImage.get());
    }

    /**
     * Starts the Kafka Connect Build on Kubernetes by creating the ConfigMap with the Dockerfile and starting the
     * builder Pod.
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace of the Connect cluster
     * @param connectBuild          KafkaConnectBuild object
     * @param dockerFileConfigMap   ConfigMap with the generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes when the build is finished (or fails if it fails)
     */
    private Future<Void> kubernetesBuildStart(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, ConfigMap dockerFileConfigMap, String newBuildRevision)  {
        return configMapOperations.reconcile(reconciliation, namespace, KafkaConnectResources.dockerFileConfigMapName(connectBuild.getCluster()), dockerFileConfigMap)
                .compose(ignore -> serviceAccountOperations.reconcile(reconciliation, namespace, KafkaConnectResources.buildServiceAccountName(connectBuild.getCluster()), connectBuild.generateServiceAccount()))
                .compose(ignore -> podOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), connectBuild.generateBuilderPod(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, newBuildRevision)))
                .mapEmpty();
    }

    /**
     * Checks if the builder Pod finished the build
     *
     @param namespace               Namespace of the Connect cluster
     * @param podName               The name of the Pod which should be checked whether it finished
     *
     * @return                      True if the build already finished, false if it is still building
     */
    private boolean kubernetesBuildPodFinished(String namespace, String podName, String containerName)   {
        Pod buildPod = podOperator.get(namespace, podName);
        return KafkaConnectBuildUtils.buildPodComplete(buildPod, containerName);
    }

    /**
     * Waits for the Kafka Connect build to finish and collects the results from it
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace of the Connect cluster
     * @param connectBuild          KafkaConnectBuild object
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    private Future<String> kubernetesBuildWaitForFinish(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild)  {
        String buildPodName = KafkaConnectResources.buildPodName(connectBuild.getCluster());
        String containerName = KafkaConnectBuildUtils.getBuildContainerName(connectBuild.getCluster(), pfa.isOpenshift());

        return podOperator.waitFor(reconciliation, namespace, buildPodName, "complete", 1_000, connectBuildTimeoutMs, (ignore1, ignore2) -> kubernetesBuildPodFinished(namespace, buildPodName, containerName))
                .compose(ignore -> podOperator.getAsync(namespace, buildPodName))
                .compose(pod -> {
                    if (KafkaConnectBuildUtils.buildPodSucceeded(pod, containerName)) {
                        ContainerStateTerminated state = KafkaConnectBuildUtils.getConnectBuildContainerStateTerminated(pod, containerName);
                        String image = state.getMessage().trim();
                        LOGGER.infoCr(reconciliation, "Build completed successfully. New image is {}.", image);
                        return Future.succeededFuture(image);
                    } else if (KafkaConnectBuildUtils.buildPodFailed(pod, buildPodName)) {
                        ContainerStateTerminated state = KafkaConnectBuildUtils.getConnectBuildContainerStateTerminated(pod, containerName);
                        LOGGER.warnCr(reconciliation, "Build failed with code {}: {}", state.getExitCode(), state.getMessage());
                    } else {
                        LOGGER.warnCr(reconciliation, "Build failed - no container with name {}", containerName);
                    }
                    return Future.failedFuture("The Kafka Connect build failed");
                });
    }

    /**
     * Executes the Kafka Connect Build on OpenShift. Run only if needed because of changes to the Dockerfile or when
     * triggered by annotation.
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace of the Connect cluster
     * @param connectBuild          KafkaConnectBuild object
     * @param forceRebuild          If true, force a new build even if one is already in progress
     * @param dockerfile            The generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    private Future<String> openShiftBuild(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, boolean forceRebuild, KafkaConnectDockerfile dockerfile, String newBuildRevision) {
        final AtomicReference<String> buildImage = new AtomicReference<>();
        return buildConfigOperator.getAsync(namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()))
                .compose(buildConfig -> {
                    if (buildConfig != null
                            && buildConfig.getStatus() != null
                            && buildConfig.getStatus().getLastVersion() != null) {
                        Long lastVersion = buildConfig.getStatus().getLastVersion();
                        return buildOperator.getAsync(namespace, KafkaConnectResources.buildName(connectBuild.getCluster(), lastVersion));
                    } else {
                        return Future.succeededFuture();
                    }
                })
                .compose(build -> {
                    if (build != null)  {
                        String existingBuildRevision = Annotations.stringAnnotation(build, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null);
                        if (newBuildRevision.equals(existingBuildRevision)
                                && !KafkaConnectBuildUtils.buildFailed(build)
                                && !forceRebuild) {
                            // Build exists, is not failed, and is building the same Dockerfile, and we are not
                            // asked to force re-build by the annotation => we re-use the existing build
                            LOGGER.infoCr(reconciliation, "Previous build exists with the same Dockerfile and will be reused.");
                            return Future.succeededFuture(build.getMetadata().getName());
                        } else {
                            // Build exists, but it either failed or is for different Dockerfile => start new build
                            return openShiftBuildStart(reconciliation, namespace, connectBuild, dockerfile, newBuildRevision);
                        }
                    } else {
                        return openShiftBuildStart(reconciliation, namespace, connectBuild, dockerfile, newBuildRevision);
                    }
                })
                .compose(buildName -> openShiftBuildWaitForFinish(reconciliation, namespace, buildName))
                .compose(image -> {
                    buildImage.set(image);
                    return podOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), null);
                }).map(ignore -> buildImage.get());
    }

    /**
     * Starts the Kafka Connect Build on OpenShift.
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace of the Connect cluster
     * @param connectBuild          KafkaConnectBuild object
     * @param dockerfile            The generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes with the build name when the build is finished (or fails if it fails)
     */
    private Future<String> openShiftBuildStart(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, KafkaConnectDockerfile dockerfile, String newBuildRevision) {
        return validateImageStream(namespace, connectBuild.getBuild().getOutput())
                .compose(ignore -> configMapOperations.reconcile(reconciliation, namespace, KafkaConnectResources.dockerFileConfigMapName(connectBuild.getCluster()), null))
                .compose(ignore -> buildConfigOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), connectBuild.generateBuildConfig(dockerfile)))
                .compose(ignore -> buildConfigOperator.startBuild(namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), connectBuild.generateBuildRequest(newBuildRevision)))
                .map(build -> build.getMetadata().getName());
    }

    /**
     * Checks if the image stream is required and exists.
     *
     * @param namespace     Namespace where the BuildConfig exists
     * @param buidlOutput   Build output configuration
     * @return              Future that completes when the check completes
     */
    public Future<Void> validateImageStream(String namespace, Output buidlOutput)   {
        if (buidlOutput != null && buidlOutput.getType().equals(Output.TYPE_IMAGESTREAM)) {
            String imageName = buidlOutput.getImage().split(":")[0];
            return imageStreamOperations.getAsync(namespace, imageName)
                .compose(is -> {
                    if (is == null) {
                        return Future.failedFuture(new InvalidConfigurationException(String.format("The build can't start because there is no image stream with name %s", imageName)));
                    } else {
                        return Future.succeededFuture();
                    }
                }).mapEmpty();
        }
        return Future.succeededFuture();
    }

    /**
     * Checks if the Build finished
     *
     * @param namespace             Namespace of the Connect cluster
     * @param buildName             Name of the Build which should be checked
     *
     * @return                      True if the Build already finished, false if it is still building
     */
    private boolean openShiftBuildFinished(String namespace, String buildName) {
        Build runningBuild = buildOperator.get(namespace, buildName);
        return KafkaConnectBuildUtils.buildComplete(runningBuild);
    }

    /**
     * Waits for the Kafka Connect build to finish and collects the results from it
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace of the Connect cluster
     * @param buildName             Name of the KafkaConnectBuild
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    private Future<String> openShiftBuildWaitForFinish(Reconciliation reconciliation, String namespace, String buildName)   {
        return buildOperator.waitFor(reconciliation, namespace, buildName, "complete", 1_000, connectBuildTimeoutMs, (ignore1, ignore2) -> openShiftBuildFinished(namespace, buildName))
                .compose(ignore -> buildOperator.getAsync(namespace, buildName))
                .compose(build -> {
                    if (KafkaConnectBuildUtils.buildSucceeded(build))   {
                        // Build completed successfully. Let's extract the new image
                        if (build.getStatus().getOutputDockerImageReference() != null
                                && build.getStatus().getOutput() != null
                                && build.getStatus().getOutput().getTo() != null
                                && build.getStatus().getOutput().getTo().getImageDigest() != null) {
                            String digest = "@" + build.getStatus().getOutput().getTo().getImageDigest();
                            String image = build.getStatus().getOutputDockerImageReference();
                            String tag = image.substring(image.lastIndexOf(":"));

                            String imageWithDigest = image.replace(tag, digest);

                            LOGGER.infoCr(reconciliation, "Build {} completed successfully. New image is {}.", buildName, imageWithDigest);
                            return Future.succeededFuture(imageWithDigest);
                        } else {
                            LOGGER.warnCr(reconciliation, "Build {} completed successfully. But the new container image was not found.", buildName);
                            return Future.failedFuture("The Kafka Connect build completed, but the new container image was not found.");
                        }
                    } else {
                        // Build failed. If the Status exists, we try to provide more detailed information
                        if (build.getStatus() != null) {
                            LOGGER.infoCr(reconciliation, "Build {} failed with code {}: {}", buildName, build.getStatus().getPhase(), build.getStatus().getLogSnippet());
                        } else {
                            LOGGER.warnCr(reconciliation, "Build {} failed for unknown reason", buildName);
                        }

                        return Future.failedFuture("The Kafka Connect build failed.");
                    }
                });
    }

    /**
     * Utility class to return the information about the Kafka Connect Build.
     */
    record BuildInfo(String image, String buildRevision) { }
}
