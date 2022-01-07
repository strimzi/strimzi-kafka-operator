/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.openshift.api.model.Build;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectBuildUtils;
import io.strimzi.operator.cluster.model.KafkaConnectDockerfile;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectBuildOperator {

    private final ReconciliationLogger logger;
    private final PodOperator podOperator;
    private final ConfigMapOperator configMapOperations;
    private final ServiceAccountOperator serviceAccountOperations;
    private final BuildConfigOperator buildConfigOperator;
    private final BuildOperator buildOperator;

    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;
    private final long connectBuildTimeoutMs;
    private final PlatformFeaturesAvailability pfa;

    public ConnectBuildOperator(PlatformFeaturesAvailability pfa, ResourceOperatorSupplier supplier, ClusterOperatorConfig config, ReconciliationLogger logger) {
        this.podOperator = supplier.podOperations;
        this.configMapOperations = supplier.configMapOperations;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.buildConfigOperator = supplier.buildConfigOperations;
        this.buildOperator = supplier.buildOperations;

        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.connectBuildTimeoutMs = config.getConnectBuildTimeoutMs();
        this.pfa = pfa;
        this.logger = logger;
    }

    /**
     * Executes the Kafka Connect Build on Kubernetes. Run only if needed because of changes to the Dockerfile or when
     * triggered by annotation.
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param connectBuild          The KafkaConnectBuild model with the build definitions
     * @param forceRebuild          If true, force a new build even if one is already in progress
     * @param dockerFileConfigMap   ConfigMap with the generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    public Future<String> kubernetesBuild(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, boolean forceRebuild, ConfigMap dockerFileConfigMap, String newBuildRevision)  {
        final AtomicReference<String> buildImage = new AtomicReference<>();
        return podOperator.getAsync(namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()))
                .compose(pod -> {
                    if (pod != null)    {
                        String existingBuildRevision = Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null);
                        if (newBuildRevision.equals(existingBuildRevision)
                                && !KafkaConnectBuildUtils.buildPodFailed(pod)
                                && !forceRebuild) {
                            // Builder pod exists, is not failed, and is building the same Dockerfile and we are not
                            // asked to force re-build by the annotation => we re-use the existing build
                            logger.infoCr(reconciliation, "Previous build exists with the same Dockerfile and will be reused.");
                            return Future.succeededFuture();
                        } else {
                            // Pod exists, but it either failed or is for different Dockerfile => start new build
                            logger.infoCr(reconciliation, "Previous build exists, but uses different Dockerfile or failed. New build will be started.");
                            return podOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), null)
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
                    return podOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), null);
                })
                .compose(ignore -> pfa.supportsS2I() ? buildConfigOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), null) : Future.succeededFuture())
                .map(ignore -> buildImage.get());
    }

    /**
     * Starts the Kafka Connect Build on Kubernetes by creating the ConfigMap with the Dockerfile and starting the
     * builder Pod.
     *
     * @param reconciliation        Reconciliation object
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param connectBuild          The KafkaConnectBuild model with the build definitions
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
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param podName               The name of the Pod which should be checked whether it finished
     *
     * @return                      True if the build already finished, false if it is still building
     */
    private boolean kubernetesBuildPodFinished(String namespace, String podName)   {
        Pod buildPod = podOperator.get(namespace, podName);
        return KafkaConnectBuildUtils.buildPodComplete(buildPod);
    }

    /**
     * Waits for the Kafka Connect build to finish and collects the results from it
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param connectBuild          The KafkaConnectBuild model with the build definitions
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    private Future<String> kubernetesBuildWaitForFinish(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild)  {
        return podOperator.waitFor(reconciliation, namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster()), "complete", 1_000, connectBuildTimeoutMs, (ignore1, ignore2) -> kubernetesBuildPodFinished(namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster())))
                .compose(ignore -> podOperator.getAsync(namespace, KafkaConnectResources.buildPodName(connectBuild.getCluster())))
                .compose(pod -> {
                    if (KafkaConnectBuildUtils.buildPodSucceeded(pod)) {
                        ContainerStateTerminated state = pod.getStatus().getContainerStatuses().get(0).getState().getTerminated();
                        String image = state.getMessage().trim();
                        logger.infoCr(reconciliation, "Build completed successfully. New image is {}.", image);
                        return Future.succeededFuture(image);
                    } else {
                        ContainerStateTerminated state = pod.getStatus().getContainerStatuses().get(0).getState().getTerminated();
                        logger.warnCr(reconciliation, "Build failed with code {}: {}", state.getExitCode(), state.getMessage());
                        return Future.failedFuture("The Kafka Connect build failed");
                    }
                });
    }

    /**
     * Executes the Kafka Connect Build on OpenShift. Run only if needed because of changes to the Dockerfile or when
     * triggered by annotation.
     *
     * @param reconciliation        The reconciliation
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param connectBuild          The KafkaConnectBuild model with the build definitions
     * @param forceRebuild          If true, force a new build even if one is already in progress
     * @param dockerfile            The generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    public Future<String> openShiftBuild(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, boolean forceRebuild, KafkaConnectDockerfile dockerfile, String newBuildRevision)   {
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
                            // Build exists, is not failed, and is building the same Dockerfile and we are not
                            // asked to force re-build by the annotation => we re-use the existing build
                            logger.infoCr(reconciliation, "Previous build exists with the same Dockerfile and will be reused.");
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
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param connectBuild          The KafkaConnectBuild model with the build definitions
     * @param dockerfile            The generated Dockerfile
     * @param newBuildRevision      New build revision (hash of the Dockerfile)
     *
     * @return                      Future which completes with the build name when the build is finished (or fails if it fails)
     */
    private Future<String> openShiftBuildStart(Reconciliation reconciliation, String namespace, KafkaConnectBuild connectBuild, KafkaConnectDockerfile dockerfile, String newBuildRevision)   {
        return configMapOperations.reconcile(reconciliation, namespace, KafkaConnectResources.dockerFileConfigMapName(connectBuild.getCluster()), null)
                .compose(ignore -> buildConfigOperator.reconcile(reconciliation, namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), connectBuild.generateBuildConfig(dockerfile)))
                .compose(ignore -> buildConfigOperator.startBuild(namespace, KafkaConnectResources.buildConfigName(connectBuild.getCluster()), connectBuild.generateBuildRequest(newBuildRevision)))
                .map(build -> build.getMetadata().getName());
    }

    /**
     * Checks if the Build finished
     *
     * @param namespace             Namespace where the Kafka Connect is deployed
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
     * @param namespace             Namespace where the Kafka Connect is deployed
     * @param buildName             Name of the KafkaConnectBuild
     *
     * @return                      Future which completes with the built image when the build is finished (or fails if it fails)
     */
    private Future<String> openShiftBuildWaitForFinish(Reconciliation reconciliation, String namespace, String buildName)   {
        return buildOperator.waitFor(reconciliation, namespace, buildName, "complete", 1_000, connectBuildTimeoutMs, (ignore1, ignore2) -> openShiftBuildFinished(namespace, buildName))
                .compose(ignore -> buildOperator.getAsync(namespace, buildName))
                .compose(build -> {
                    if (KafkaConnectBuildUtils.buildSucceeded(build))   {
                        // Build completed successfully. Lets extract the new image
                        if (build.getStatus().getOutputDockerImageReference() != null
                                && build.getStatus().getOutput() != null
                                && build.getStatus().getOutput().getTo() != null
                                && build.getStatus().getOutput().getTo().getImageDigest() != null) {
                            String digest = "@" + build.getStatus().getOutput().getTo().getImageDigest();
                            String image = build.getStatus().getOutputDockerImageReference();
                            String tag = image.substring(image.lastIndexOf(":"));

                            String imageWithDigest = image.replace(tag, digest);

                            logger.infoCr(reconciliation, "Build {} completed successfully. New image is {}.", buildName, imageWithDigest);
                            return Future.succeededFuture(imageWithDigest);
                        } else {
                            logger.warnCr(reconciliation, "Build {} completed successfully. But the new container image was not found.", buildName);
                            return Future.failedFuture("The Kafka Connect build completed, but the new container image was not found.");
                        }
                    } else {
                        // Build failed. If the Status exists, we try to provide more detailed information
                        if (build.getStatus() != null) {
                            logger.infoCr(reconciliation, "Build {} failed with code {}: {}", buildName, build.getStatus().getPhase(), build.getStatus().getLogSnippet());
                        } else {
                            logger.warnCr(reconciliation, "Build {} failed for unknown reason", buildName);
                        }

                        return Future.failedFuture("The Kafka Connect build failed.");
                    }
                });
    }
}
