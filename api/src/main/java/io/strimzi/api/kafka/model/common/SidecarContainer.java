/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures a sidecar container for inclusion in Kafka pods. This abstraction
 * is needed rather than using the standard Kubernetes container model to
 * avoid CRD generation issues with IntOrString fields while providing
 * essential container configuration options.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"name", "image", "imagePullPolicy", "command", "args", "env", "volumeMounts", "ports", "resources", "securityContext", "livenessProbe", "readinessProbe"})
@EqualsAndHashCode
@ToString
public class SidecarContainer implements UnknownPropertyPreserving {
    private String name;
    private String image;
    private String imagePullPolicy;
    private List<String> command;
    private List<String> args;
    private List<ContainerEnvVar> env;
    private List<VolumeMount> volumeMounts;
    private List<ContainerPort> ports;
    private ResourceRequirements resources;
    private SecurityContext securityContext;
    private SidecarProbe livenessProbe;
    private SidecarProbe readinessProbe;
    private Map<String, Object> additionalProperties;

    @Description("The name of the sidecar container.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("The container image.")
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("The image pull policy for the container image. " +
                "More info: https://kubernetes.io/docs/concepts/containers/images#image-pull-policy")
    @KubeLink(group = "core", version = "v1", kind = "container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getImagePullPolicy() {
        return imagePullPolicy;
    }

    public void setImagePullPolicy(String imagePullPolicy) {
        this.imagePullPolicy = imagePullPolicy;
    }

    @Description("Command to execute in the container.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getCommand() {
        return command;
    }

    public void setCommand(List<String> command) {
        this.command = command;
    }

    @Description("Arguments to the entrypoint.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    @Description("Environment variables to set in the container.")
    @KubeLink(group = "core", version = "v1", kind = "envvar")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<ContainerEnvVar> getEnv() {
        return env;
    }

    public void setEnv(List<ContainerEnvVar> env) {
        this.env = env;
    }

    @Description("Volume mounts to add to the container.")
    @KubeLink(group = "core", version = "v1", kind = "volumemount")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<VolumeMount> getVolumeMounts() {
        return volumeMounts;
    }

    public void setVolumeMounts(List<VolumeMount> volumeMounts) {
        this.volumeMounts = volumeMounts;
    }

    @Description("List of ports to expose from the sidecar container.")
    @KubeLink(group = "core", version = "v1", kind = "containerport")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<ContainerPort> getPorts() {
        return ports;
    }

    public void setPorts(List<ContainerPort> ports) {
        this.ports = ports;
    }

    @Description("Resource requirements for the sidecar container.")
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @Description("Security context for the sidecar container.")
    @KubeLink(group = "core", version = "v1", kind = "securitycontext")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public void setSecurityContext(SecurityContext securityContext) {
        this.securityContext = securityContext;
    }

    @Description("Liveness probe for the sidecar container.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public SidecarProbe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(SidecarProbe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @Description("Readiness probe for the sidecar container.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public SidecarProbe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(SidecarProbe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
