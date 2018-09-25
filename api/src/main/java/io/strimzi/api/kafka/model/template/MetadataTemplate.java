/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Metadata template for Strimzi pods, statefulsets, deployments and services.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "labels", "annotations"})
public class MetadataTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, String> labels = new HashMap<>(0);
    private Map<String, String> annotations = new HashMap<>(0);

    @Description("Labels which should be added to the resource template. " +
            "Can be applied to different resources such as `StatefulSets`, `Deployments`, `Pods`, and `Services`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    @Description("Annotations which should be added to the resource template. " +
            "Can be applied to different resources such as `StatefulSets`, `Deployments`, `Pods`, and `Services`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
    }
}
