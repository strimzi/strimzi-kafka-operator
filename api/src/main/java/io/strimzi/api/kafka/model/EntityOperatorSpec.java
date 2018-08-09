/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.Affinity;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Entity Operator deployment.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntityOperatorSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_TLS_SIDECAR_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE", "strimzi/entity-operator-stunnel:latest");
    public static final int DEFAULT_REPLICAS = 1;

    private TopicOperatorSpec topicOperator;
    private UserOperatorSpec userOperator;
    private Affinity affinity;
    private Sidecar tlsSidecar;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Configuration of the Topic Operator")
    public TopicOperatorSpec getTopicOperator() {
        return topicOperator;
    }

    public void setTopicOperator(TopicOperatorSpec topicOperator) {
        this.topicOperator = topicOperator;
    }

    @Description("Configuration of the User Operator")
    public UserOperatorSpec getUserOperator() {
        return userOperator;
    }

    public void setUserOperator(UserOperatorSpec userOperator) {
        this.userOperator = userOperator;
    }

    @Description("Pod affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Sidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(Sidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
