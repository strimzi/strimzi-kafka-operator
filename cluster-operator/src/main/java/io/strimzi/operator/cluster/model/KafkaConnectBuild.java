/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildOutput;
import io.fabric8.openshift.api.model.BuildOutputBuilder;
import io.fabric8.openshift.api.model.BuildRequest;
import io.fabric8.openshift.api.model.BuildRequestBuilder;
import io.fabric8.openshift.api.model.DockerBuildStrategyBuilder;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.connect.build.Build;
import io.strimzi.api.kafka.model.connect.build.DockerOutput;
import io.strimzi.api.kafka.model.connect.build.ImageStreamOutput;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Model class for Kafka Connect Build - this model handled the build of the new image with custom connectors
 */
public class KafkaConnectBuild extends AbstractModel {
    protected static final String COMPONENT_TYPE = "kafka-connect-build";

    /*test*/ static final String DEFAULT_KANIKO_EXECUTOR_IMAGE = "gcr.io/kaniko-project/executor:latest";
    /*test*/ static final String DEFAULT_BUILDAH_IMAGE = "quay.io/strimzi/buildah:latest";

    protected static final String CO_ENV_VAR_CUSTOM_CONNECT_BUILD_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_CONNECT_BUILD_LABELS";

    private Build build;
    private Map<String, String> templateBuildConfigLabels;
    private Map<String, String> templateBuildConfigAnnotations;
    private PodTemplate templatePod;
    /*test*/ String baseImage;
    private List<String> additionalBuildOptions;
    private List<String> additionalPushOptions;

    private String pullSecret;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_CONNECT_BUILD_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider sharedEnvironmentProvider provider
     */
    protected KafkaConnectBuild(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider, boolean useConnectBuildWithBuildah) {
        super(reconciliation, resource, KafkaConnectResources.buildPodName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);

        // TODO: use configuration from the `ClusterOperatorConfig` rather than from env variables directly - https://github.com/strimzi/strimzi-kafka-operator/issues/11981
        if (useConnectBuildWithBuildah) {
            this.image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_BUILDAH_IMAGE, DEFAULT_BUILDAH_IMAGE);
        } else {
            this.image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE, DEFAULT_KANIKO_EXECUTOR_IMAGE);
        }
    }

    /**
     * Creates the KafkaConnectBuild instance from the Kafka Connect Custom Resource.
     *
     * @param reconciliation                The reconciliation
     * @param kafkaConnect                  Kafka Connect CR with the build configuration
     * @param versions                      Kafka versions configuration
     * @param sharedEnvironmentProvider     Shared environment provider
     * @param useConnectBuildWithBuildah    determines if Buildah should be used for the Connect Build
     * @return              Instance of KafkaConnectBuild class
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "deprecation"})
    public static KafkaConnectBuild fromCrd(Reconciliation reconciliation,
                                            KafkaConnect kafkaConnect,
                                            KafkaVersion.Lookup versions,
                                            SharedEnvironmentProvider sharedEnvironmentProvider,
                                            boolean useConnectBuildWithBuildah) {
        KafkaConnectBuild result = new KafkaConnectBuild(reconciliation, kafkaConnect, sharedEnvironmentProvider, useConnectBuildWithBuildah);
        KafkaConnectSpec spec = kafkaConnect.getSpec();

        if (spec == null) {
            throw new InvalidResourceException("Required .spec section is missing.");
        }

        if (spec.getBuild() != null)    {
            validateBuildConfiguration(spec.getBuild());

            // The additionalKanikoOptions are validated separately to avoid parsing the list twice
            if (spec.getBuild().getOutput() != null
                    && spec.getBuild().getOutput() instanceof DockerOutput dockerOutput) {

                // Validation to check if KafkaConnect .spec.image equals .spec.build.output.image name
                if (dockerOutput.getImage() != null && spec.getImage() != null && dockerOutput.getImage().equals(spec.getImage())) {
                    throw new InvalidResourceException("KafkaConnect .spec.image cannot be the same as .spec.build.output.image");
                }
                if (useConnectBuildWithBuildah) {
                    if (dockerOutput.getAdditionalBuildOptions() != null
                            && !dockerOutput.getAdditionalBuildOptions().isEmpty()) {
                        validateAdditionalOptions(DockerOutput.ALLOWED_BUILDAH_BUILD_OPTIONS, dockerOutput.getAdditionalBuildOptions(), ".spec.build.output.additionalBuildOptions");
                        result.additionalBuildOptions = dockerOutput.getAdditionalBuildOptions();
                    }
                    if (dockerOutput.getAdditionalPushOptions() != null
                            && !dockerOutput.getAdditionalPushOptions().isEmpty()) {
                        validateAdditionalOptions(DockerOutput.ALLOWED_BUILDAH_PUSH_OPTIONS, dockerOutput.getAdditionalPushOptions(), ".spec.build.output.additionalPushOptions");
                        result.additionalPushOptions = dockerOutput.getAdditionalPushOptions();
                    }
                } else {
                    if (dockerOutput.getAdditionalBuildOptions() != null
                            && !dockerOutput.getAdditionalBuildOptions().isEmpty()) {
                        // in case that we are using Kaniko and `.additionalBuildOptions` field contains some options, we want to check them
                        // because `.additionalKanikoOptions` is deprecated and `.additionalBuildOptions` is replacement
                        validateAdditionalOptions(DockerOutput.ALLOWED_KANIKO_OPTIONS, dockerOutput.getAdditionalBuildOptions(), ".spec.build.output.additionalBuildOptions");
                        result.additionalBuildOptions = dockerOutput.getAdditionalBuildOptions();
                    } else if (dockerOutput.getAdditionalKanikoOptions() != null
                            && !dockerOutput.getAdditionalKanikoOptions().isEmpty()) {
                        validateAdditionalOptions(DockerOutput.ALLOWED_KANIKO_OPTIONS, dockerOutput.getAdditionalKanikoOptions(), ".spec.build.output.additionalKanikoOptions");
                        result.additionalBuildOptions = dockerOutput.getAdditionalKanikoOptions();
                    }
                }
            }

            result.resources = spec.getBuild().getResources();
        }

        result.baseImage = versions.kafkaConnectVersion(spec.getImage(), spec.getVersion());

        if (spec.getTemplate() != null) {
            KafkaConnectTemplate template = spec.getTemplate();

            if (template.getBuildConfig() != null) {
                result.pullSecret = template.getBuildConfig().getPullSecret();

                if (template.getBuildConfig().getMetadata() != null) {
                    if (template.getBuildConfig().getMetadata().getLabels() != null) {
                        result.templateBuildConfigLabels = template.getBuildConfig().getMetadata().getLabels();
                    }

                    if (template.getBuildConfig().getMetadata().getAnnotations() != null) {
                        result.templateBuildConfigAnnotations = template.getBuildConfig().getMetadata().getAnnotations();
                    }
                }
            }

            result.templatePod = template.getBuildPod();
            result.templateServiceAccount = template.getBuildServiceAccount();
            result.templateContainer = template.getBuildContainer();
        }

        result.build = spec.getBuild();

        return result;
    }

    /**
     * Validates the Build configuration to check unique connector names etc.
     *
     * @param build     Kafka Connect Build configuration
     */
    private static void validateBuildConfiguration(Build build)    {
        if (build.getPlugins() == null) {
            throw new InvalidResourceException("List of connector plugins is required when Kafka Connect Build is used.");
        }

        List<String> names = build.getPlugins().stream().map(Plugin::getName).distinct().toList();

        if (names.size() != build.getPlugins().size())  {
            throw new InvalidResourceException("Connector plugins names have to be unique within a single KafkaConnect resource.");
        }

        for (Plugin plugin : build.getPlugins())    {
            if (plugin.getArtifacts() == null)  {
                throw new InvalidResourceException("Each connector plugin needs to have a list of artifacts.");
            }
        }
    }

    /**
     * Validates the additional Buildah and Kaniko options configured by the user against the list of allowed options.
     * If there is a not allowed option found, it raises and InvalidResourceException exception.
     *
     * @param allowedOptions    allowed options for particular operation - build/push - for Buildah or Kaniko.
     * @param desiredOptions    list of desired options by the user.
     * @param specPath          path to options in `.spec` section - used when throwing exception.
     */
    private static void validateAdditionalOptions(String allowedOptions, List<String> desiredOptions, String specPath) {
        List<String> customAllowedOptions = Arrays.asList(allowedOptions.split("\\s*,+\\s*"));
        List<String> forbiddenOptions = desiredOptions.stream()
            .map(option -> option.contains("=") ? option.substring(0, option.indexOf("=")) : option)
            .filter(option -> customAllowedOptions.stream().noneMatch(option::equals))
            .toList();

        if (!forbiddenOptions.isEmpty()) {
            throw new InvalidResourceException(specPath + " contains forbidden options: " + forbiddenOptions);
        }
    }

    /**
     * Returns the build configuration of the KafkaConnect CR
     *
     * @return  Kafka Connect build configuration
     */
    public Build getBuild() {
        return build;
    }

    /**
     * Generates a ConfigMap with the Dockerfile used for the build
     *
     * @param dockerfile    Instance of the KafkaConnectDockerfile class with the prepared Dockerfile
     *
     * @return  ConfigMap with the Dockerfile
     */
    public ConfigMap generateDockerfileConfigMap(KafkaConnectDockerfile dockerfile)   {
        return ConfigMapUtils.createConfigMap(
                KafkaConnectResources.dockerFileConfigMapName(cluster),
                namespace,
                labels,
                ownerReference,
                Collections.singletonMap("Dockerfile", dockerfile.getDockerfile())
        );
    }

    /**
     * Generates the Dockerfile based on the Kafka Connect build configuration.
     *
     * @return  Instance of the KafkaConnectDockerfile class with the prepared Dockerfile
     */
    public KafkaConnectDockerfile generateDockerfile()  {
        return new KafkaConnectDockerfile(baseImage, build, sharedEnvironmentProvider);
    }

    /**
     * Generates builder Pod for building a new KafkaConnect container image with additional connector plugins
     *
     * @param isOpenShift       Flag defining whether we are running on OpenShift
     * @param isBuildahBuild    Flag defining whether we should use Buildah or Kaniko (based on Feature Gate).
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  Image pull secrets
     * @param newBuildRevision  Revision of the build which will be build used for annotation
     *
     * @return  Pod which will build the new container image
     */
    public Pod generateBuilderPod(boolean isOpenShift, boolean isBuildahBuild, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, String newBuildRevision) {
        return WorkloadUtils.createPod(
                componentName,
                namespace,
                labels,
                ownerReference,
                templatePod,
                DEFAULT_POD_LABELS,
                Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, newBuildRevision),
                templatePod != null ? templatePod.getAffinity() : null,
                null,
                List.of(createContainer(imagePullPolicy, isBuildahBuild)),
                getVolumes(isOpenShift),
                imagePullSecrets,
                securityProvider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
        );
    }

    /**
     * Generates a list of volumes used by the builder pod
     *
     * @param isOpenShift   Flag defining whether we are running on OpenShift
     *
     * @return  List of volumes
     */
    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumes = new ArrayList<>(2);

        volumes.add(VolumeUtils.createConfigMapVolume("dockerfile", KafkaConnectResources.dockerFileConfigMapName(cluster), Collections.singletonMap("Dockerfile", "Dockerfile")));

        if (build.getOutput() instanceof DockerOutput output) {

            if (output.getPushSecret() != null) {
                volumes.add(VolumeUtils.createSecretVolume("docker-credentials", output.getPushSecret(), Collections.singletonMap(".dockerconfigjson", "config.json"), isOpenShift));
            }
        } else {
            throw new RuntimeException("Kubernetes build requires output of type `docker`.");
        }
        
        TemplateUtils.addAdditionalVolumes(templatePod, volumes);

        return volumes;
    }

    /**
     * Generates a list of volume mounts used by the builder pod
     *
     * @return  List of volume mounts
     */
    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMounts = new ArrayList<>(2);

        volumeMounts.add(new VolumeMountBuilder().withName("dockerfile").withMountPath("/dockerfile").build());

        if (build.getOutput() instanceof DockerOutput output) {

            if (output.getPushSecret() != null) {
                volumeMounts.add(new VolumeMountBuilder().withName("docker-credentials").withMountPath("/kaniko/.docker").build());
            }
        } else {
            throw new RuntimeException("Kubernetes build requires output of type `docker`.");
        }
        TemplateUtils.addAdditionalVolumeMounts(volumeMounts, templateContainer);

        return volumeMounts;
    }

    /**
     * Generates list of container environment variables for the builder pod. Currently contains only the proxy env vars
     * and any user defined vars from the templates.
     *
     * @return  List of environment variables
     */
    private List<EnvVar> getBuildContainerEnvVars() {
        // Add shared environment variables used for all containers
        List<EnvVar> varList = new ArrayList<>(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    /**
     * Generates the builder container for Buildah or Kaniko.
     *
     * @param imagePullPolicy   Image pull policy.
     * @param isBuildahBuild    Flag defining whether we should use Buildah or Kaniko (based on Feature Gate).
     *
     * @return  Builder container definition which will be used in the Pod.
     */
    Container createContainer(ImagePullPolicy imagePullPolicy, boolean isBuildahBuild) {
        return ContainerUtils.createContainer(
            componentName,
            image,
            isBuildahBuild ? buildahArguments() : kanikoArguments(),
            securityProvider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
            resources,
            getBuildContainerEnvVars(),
            null,
            getVolumeMounts(),
            null,
            null,
            imagePullPolicy
        );
    }

    /**
     * Returns Kaniko arguments needed for the build (and push) of the image - used inside the container.
     *
     * @return Kaniko arguments needed for the build (and push) of the image - used inside the container.
     */
    List<String> kanikoArguments() {
        List<String> args = additionalBuildOptions != null ? new ArrayList<>(3 + additionalBuildOptions.size()) : new ArrayList<>(4);
        args.add("--dockerfile=/dockerfile/Dockerfile");
        args.add("--image-name-with-digest-file=/dev/termination-log");
        args.add("--destination=" + build.getOutput().getImage());

        if (additionalBuildOptions != null) {
            args.addAll(additionalBuildOptions);
        }

        return args;
    }

    /**
     * Returns Buildah arguments needed for the build and push of the image - used inside the container.
     *
     * @return Buildah arguments needed for the build and push of the image - used inside the container.
     */
    List<String> buildahArguments() {
        List<String> args = new ArrayList<>(3);
        args.add("/bin/bash");
        args.add("-ec");

        final String buildOpts = additionalBuildOptions != null ? String.join(" ", additionalBuildOptions) : "";
        final String pushOpts = additionalPushOptions != null ? String.join(" ", additionalPushOptions) : "";
        final String outputImage = build.getOutput().getImage();

        args.add(
            "buildah build --file=/dockerfile/Dockerfile --tag=" + outputImage + " --storage-driver=overlay " + buildOpts + "\n" +
            "buildah push --storage-driver=overlay --digestfile=/tmp/digest " + pushOpts + " " + outputImage + "\n" +
            "echo \"$(buildah images --storage-driver=overlay --format '{{.Name}}' " + outputImage + ")@$(cat /tmp/digest)\" > /dev/termination-log"
        );

        return args;
    }

    /**
     * Generates a BuildConfig which will be used to build new container images with additional connector plugins on OCP.
     *
     * @param dockerfile    Dockerfile which should be built by the BuildConfig
     *
     * @return  OpenShift BuildConfig for building new container images on OpenShift
     */
    public BuildConfig generateBuildConfig(KafkaConnectDockerfile dockerfile)    {
        BuildOutput output;

        if (build.getOutput() instanceof DockerOutput dockerOutput) {

            output = new BuildOutputBuilder()
                    .withNewTo()
                        .withKind("DockerImage")
                        .withName(dockerOutput.getImage())
                    .endTo()
                    .build();

            if (dockerOutput.getPushSecret() != null) {
                output.setPushSecret(new LocalObjectReferenceBuilder().withName(dockerOutput.getPushSecret()).build());
            }
        } else if (build.getOutput() instanceof ImageStreamOutput imageStreamOutput)  {

            output = new BuildOutputBuilder()
                    .withNewTo()
                        .withKind("ImageStreamTag")
                        .withName(imageStreamOutput.getImage())
                    .endTo()
                    .build();
        } else {
            throw new RuntimeException("Unknown output type " + build.getOutput().getType());
        }

        DockerBuildStrategyBuilder dockerBuildStrategyBuilder = new DockerBuildStrategyBuilder();
        if (pullSecret != null) {
            dockerBuildStrategyBuilder.withNewPullSecret().withName(pullSecret).endPullSecret();
        }

        return new BuildConfigBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildConfigName(cluster))
                    .withLabels(labels.withAdditionalLabels(templateBuildConfigLabels).toMap())
                    .withAnnotations(templateBuildConfigAnnotations)
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withOutput(output)
                    .withNewSource()
                        .withType("Dockerfile")
                        .withDockerfile(dockerfile.getDockerfile())
                    .endSource()
                    .withNewStrategy()
                        .withType("Docker")
                        .withDockerStrategy(dockerBuildStrategyBuilder.build())
                    .endStrategy()
                    .withResources(build.getResources())
                    .withRunPolicy("Serial")
                    .withFailedBuildsHistoryLimit(5)
                    .withSuccessfulBuildsHistoryLimit(5)
                    .withFailedBuildsHistoryLimit(5)
                .endSpec()
                .build();
    }

    /**
     * Generates OpenShift Build Request to start a new build using OpenShift Build feature
     *
     * @param buildRevision The revision of the build (to indicate if rebuild is needed)
     *
     * @return  The BuildRequest resource
     */
    public BuildRequest generateBuildRequest(String buildRevision)  {
        return new BuildRequestBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildConfigName(cluster))
                    .withNamespace(namespace)
                    .withAnnotations(Collections.singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildRevision))
                    .withLabels(labels.withAdditionalLabels(templateBuildConfigLabels).toMap())
                .endMetadata()
                .build();
    }
}
