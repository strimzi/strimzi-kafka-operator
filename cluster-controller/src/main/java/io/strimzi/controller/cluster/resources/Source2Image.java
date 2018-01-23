package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.openshift.api.model.BinaryBuildSource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildTriggerPolicy;
import io.fabric8.openshift.api.model.ImageChangeTrigger;
import io.fabric8.openshift.api.model.ImageLookupPolicyBuilder;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.api.model.TagReference;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents S2I upgrade on top of the regular resource
 */
public class Source2Image {
    private final String name;
    private final String namespace;
    private final String sourceImage;
    private final String tag;
    private final String targetImage;
    protected Map<String, String> labels;

    // Annotations
    public static String ANNOTATION_S2I = "s2i";
    public static String ANNOTATION_RESOLVE_NAMES = "alpha.image.policy.openshift.io/resolve-names";

    // Keys to config JSON
    private static String KEY_SOURCE_IMAGE = "sourceImage";
    public static String KEY_ENABLED = "enabled";

    // Default values
    private static String DEFAULT_SOURCE_IMAGE = "strimzi/kafka-connect-s2i:latest";

    /**
     * Full constructor
     *
     * @param namespace     OpenShift project
     * @param name          Name od the Source2Image resources (should be the KafkaConnectResource name)
     * @param labels        Maps with labels
     * @param sourceImage   Name of the source Docker image
     */
    public Source2Image(String namespace, String name, Map<String, String> labels, String sourceImage) {
        this.name = name;
        this.namespace = namespace;
        this.labels = labels;
        this.sourceImage = sourceImage.substring(0, sourceImage.lastIndexOf(":"));
        this.tag = sourceImage.substring(sourceImage.lastIndexOf(":") + 1);
        this.targetImage = name;
    }

    /**
     * Constructor which can be used for deleting the Source2Image objects based only on name and namsespace
     *
     * @param namespace     OpenShift project
     * @param name          Name od the Source2Image resources (should be the KafkaConnectResource name)
     */
    public Source2Image(String namespace, String name) {
        this.name = name;
        this.namespace = namespace;
        this.labels = null;
        this.sourceImage = null;
        this.tag = null;
        this.targetImage = null;
    }

    /**
     * Create Source2Image object from JSON object
     *
     * @param namespace     OpenShift project
     * @param name          Name of the Source2Image resources
     * @param labels        Map with labels
     * @param config        JsonObject with configuration
     * @return              Source2Image instance
     */
    public static Source2Image fromJson(String namespace, String name, Map<String, String> labels, JsonObject config) {
        String sourceImage = config.getString(KEY_SOURCE_IMAGE, DEFAULT_SOURCE_IMAGE);
        return new Source2Image(namespace, name, labels, sourceImage);
    }

    /**
     * Create Source2Image object from existing OpenShift resources
     *
     * @param namespace     OpenShift project
     * @param name          Name of the Source2Image resources
     * @param os            OpenShift utils
     * @return              Source2Image instance
     */
    public static Source2Image fromOpenShift(String namespace, String name, OpenShiftUtils os) {
        ImageStream sis = (ImageStream) os.get(namespace, getSourceImageStreamName(name), ImageStream.class);
        String sourceImage = sis.getSpec().getTags().get(0).getFrom().getName() + ":" + sis.getSpec().getTags().get(0).getName();

        return new Source2Image(namespace, name, sis.getMetadata().getLabels(), sourceImage);
    }

    /**
     * Get the Source2Image name
     *
     * @return      Name of the Source2Image instance
     */
    public String getName() {
        return name;
    }

    /**
     * Get OpenShift project / namespace
     *
     * @return      OpenShift project / namespace of this Source2Image instance
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Get the S2I source Docker image
     *
     * @return      S2I Docker image which is used as a source for the build. The image name includes the repository /
     * organization. It doesn't include the tag.
     */
    public String getSourceImage() {
        return sourceImage;
    }

    /**
     * Get the name of the target image
     *
     * @return      Name of the target Docker image including tag
     */
    public String getTargetImage() {
        return targetImage + ":" + tag;
    }

    /**
     * Get labels used by this Source2Image instance
     *
     * @return      Map with lables
     */
    public Map<String, String> getLabels() {
        return labels;
    }

    /**
     * Get the source ImageStream name for given instance
     *
     * @return      name of the source ImageStream resource
     */
    public String getSourceImageStreamName() {
        return getSourceImageStreamName(name);
    }

    /**
     * Generates the name of the source ImageStream
     *
     * @param baseName       Name of the Source2Image resource
     * @return               Name of the source ImageStream instance
     */
    public static String getSourceImageStreamName(String baseName) {
        return baseName + "-source";
    }

    /**
     * Generate new source ImageStream for this Source2Image object
     *
     * @return      Source ImageStream resource definition
     */
    public ImageStream generateSourceImageStream() {
        ObjectReference image = new ObjectReference();
        image.setKind("DockerImage");
        image.setName(sourceImage);

        TagReference sourceTag = new TagReference();
        sourceTag.setName(tag);
        sourceTag.setFrom(image);

        ImageStream imageStream = new ImageStreamBuilder()
                .withNewMetadata()
                .withName(getSourceImageStreamName())
                .withNamespace(namespace)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withLookupPolicy(new ImageLookupPolicyBuilder().withLocal(false).build())
                .withTags(sourceTag)
                .endSpec()
                .build();

        return imageStream;
    }

    /**
     * Generate new target ImageStream for this Source2Image object
     *
     * @return      Target ImageStream resource definition
     */
    public ImageStream generateTargetImageStream() {
        ImageStream imageStream = new ImageStreamBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withLookupPolicy(new ImageLookupPolicyBuilder().withLocal(true).build())
                .endSpec()
                .build();

        return imageStream;
    }

    /**
     * Generate new BuildConfig for this Source2Image object
     *
     * @return      BuildConfig resource definition
     */
    public BuildConfig generateBuildConfig() {
        BuildTriggerPolicy triggerConfigChange = new BuildTriggerPolicy();
        triggerConfigChange.setType("ConfigChange");

        BuildTriggerPolicy triggerImageChange = new BuildTriggerPolicy();
        triggerImageChange.setType("ImageChange");
        triggerImageChange.setImageChange(new ImageChangeTrigger());

        BuildConfig build = new BuildConfigBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withFailedBuildsHistoryLimit(5)
                .withNewOutput()
                .withNewTo()
                .withKind("ImageStreamTag")
                .withName(getTargetImage())
                .endTo()
                .endOutput()
                .withRunPolicy("Serial")
                .withNewSource()
                .withType("Binary")
                .withBinary(new BinaryBuildSource())
                .endSource()
                .withNewStrategy()
                .withType("Source")
                .withNewSourceStrategy()
                .withNewFrom()
                .withKind("ImageStreamTag")
                .withName(getSourceImageStreamName() + ":" + tag)
                .endFrom()
                .endSourceStrategy()
                .endStrategy()
                .withTriggers(triggerConfigChange, triggerImageChange)
                .endSpec()
                .build();

        return build;
    }

    /**
     * Patches existing source ImageStream with latest changes
     *
     * @param is    Existing source ImageStream which should be patched
     * @return      Patched ImageStream resource definition
     */
    public ImageStream patchSourceImageStream(ImageStream is) {
        is.getMetadata().setLabels(getLabels());
        is.getSpec().getTags().get(0).setName(tag);
        is.getSpec().getTags().get(0).getFrom().setName(sourceImage);

        return is;
    }

    /**
     * Patches existing target ImageStream with latest changes
     *
     * @param is    Existing target ImageStream which should be patched
     * @return      Patched ImageStream resource definition
     */
    public ImageStream patchTargetImageStream(ImageStream is) {
        is.getMetadata().setLabels(getLabels());

        return is;
    }

    /**
     * Patches existing BuildConfig with latest changes
     *
     * @param bc    Existing BuildConfig which should be patched
     * @return      Patched BuildConfig resource definition
     */
    public BuildConfig patchBuildConfig(BuildConfig bc) {
        bc.getMetadata().setLabels(getLabels());
        bc.getSpec().getOutput().getTo().setName(getTargetImage());
        bc.getSpec().getStrategy().getSourceStrategy().getFrom().setName(getSourceImageStreamName() + ":" + tag);

        return bc;
    }

    /**
     * Calculates the difference between this Source2Image instance and actual OpenShift resources
     *
     * @param os       OpenShiftUtils client
     * @return         ClusterDiffResult instance describing the differences between desired and actual Source2Image
     */
    public ClusterDiffResult diff(OpenShiftUtils os) {
        ClusterDiffResult diff = new ClusterDiffResult();

        ImageStream sis = (ImageStream) os.get(namespace, getSourceImageStreamName(), ImageStream.class);
        ImageStream tis = (ImageStream) os.get(namespace, getName(), ImageStream.class);
        BuildConfig bc = (BuildConfig) os.get(namespace, getName(), BuildConfig.class);

        if (!getLabels().equals(sis.getMetadata().getLabels())
                || !getLabels().equals(tis.getMetadata().getLabels())
                || !getLabels().equals(bc.getMetadata().getLabels())) {
            diff.setDifferent(true);
        }

        if (!getTargetImage().equals(bc.getSpec().getOutput().getTo().getName())
                || !(getSourceImageStreamName() + ":" + tag).equals(bc.getSpec().getStrategy().getSourceStrategy().getFrom().getName()))    {
            diff.setDifferent(true);
        }

        if (!tag.equals(sis.getSpec().getTags().get(0).getName())
                || !sourceImage.equals(sis.getSpec().getTags().get(0).getFrom().getName()))   {
            diff.setDifferent(true);
        }

        return diff;
    }

    /**
     * Enum for passing Diff resources in ClusterDiffResult class
     */
    public enum Source2ImageDiff {
        /**
         * Soruce2Image should be deleted
         */
        DELETE,

        /**
        * Source2Image should be checked for updates
        */
        UPDATE,

        /**
         * Source2Image should be created
         */
        CREATE,

        /**
         * No changes to Source2Image
         */
        NONE;
    }
}
