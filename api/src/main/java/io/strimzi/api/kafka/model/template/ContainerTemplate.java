/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a template for Strimzi containers.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@DescriptionFile
@EqualsAndHashCode
public class ContainerTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private List<ContainerEnvVar> env;
    private SecurityContext securityContext;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Environment variables which should be applied to the container.")
    public List<ContainerEnvVar> getEnv() {
        return env;
    }

    public void setEnv(List<ContainerEnvVar> env) {
        this.env = env;
    }

    @Description("Security context for the container")
    @KubeLink(group = "core", version = "v1", kind = "securitycontext")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public void setSecurityContext(SecurityContext securityContext) {
        this.securityContext = securityContext;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
