/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.Resources;
import io.sundr.builder.annotations.Buildable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a pod template for Strimzi resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "name", "image", "imagePullPolicy", "env",
        "livenessProbe", "readinessProbe", "resources"})
public class ContainerTemplate {
    private static final long serialVersionUID = 1L;

    private String name;
    private String image;
    private String imagePullPolicy;
    private List<EnvVar> env;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private Resources resources;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getImagePullPolicy() {
        return imagePullPolicy;
    }

    public void setImagePullPolicy(String imagePullPolicy) {
        this.imagePullPolicy = imagePullPolicy;
    }

    public List<EnvVar> getEnv() {
        return env;
    }

    public void setEnv(List<EnvVar> env) {
        this.env = env;
    }

    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    public Resources getResources() {
        return resources;
    }

    public void setResources(Resources resources) {
        this.resources = resources;
    }
}
