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
     * Constructor
     *
     * @param namespace     OpenShift project
     * @param name       Name od the name
     * @param sourceImage   Name of the sourceDocker image
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
     * @param name       Name od the name
     */
    public Source2Image(String namespace, String name) {
        this.name = name;
        this.namespace = namespace;
        this.labels = null;
        this.sourceImage = null;
        this.tag = null;
        this.targetImage = null;
    }

    public static Source2Image fromJson(String name, String namespace, Map<String, String> labels, JsonObject config) {
        String sourceImage = config.getString(KEY_SOURCE_IMAGE, DEFAULT_SOURCE_IMAGE);
        return new Source2Image(name, namespace, labels, sourceImage);
    }

    public JsonObject toJson()  {
        return new JsonObject().put(KEY_ENABLED, true).put(KEY_SOURCE_IMAGE, getSourceImage() + ":" + tag);
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSourceImage() {
        return sourceImage;
    }

    public String getTargetImage() {
        return targetImage + ":" + tag;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public String getSourceImageStreamName() {
        return name + "-source";
    }

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

    public ImageStream patchSourceImageStream(ImageStream is) {
        is.getMetadata().setLabels(getLabels());
        is.getSpec().getTags().get(0).setName(tag);
        is.getSpec().getTags().get(0).getFrom().setName(sourceImage);

        return is;
    }

    public ImageStream patchTargetImageStream(ImageStream is) {
        is.getMetadata().setLabels(getLabels());

        return is;
    }

    public BuildConfig patchBuildConfig(BuildConfig bc) {
        bc.getMetadata().setLabels(getLabels());
        bc.getSpec().getOutput().getTo().setName(getTargetImage());
        bc.getSpec().getStrategy().getSourceStrategy().getFrom().setName(getSourceImageStreamName() + ":" + tag);

        return bc;
    }


    public enum Source2ImageDiff {
        DELETE, UPDATE, CREATE, NONE;
    }
}
