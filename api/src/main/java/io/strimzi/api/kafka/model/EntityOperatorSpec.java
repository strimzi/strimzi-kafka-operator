/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Toleration;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of the Entity Operator deployment.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "topicOperator", "userOperator", "affinity",
        "tolerations", "tlsSidecar"})
public class EntityOperatorSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_TLS_SIDECAR_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE", "strimzi/entity-operator-stunnel:latest");
    public static final int DEFAULT_REPLICAS = 1;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;

    private EntityTopicOperatorSpec topicOperator;
    private EntityUserOperatorSpec userOperator;
    private Affinity affinity;
    private List<Toleration> tolerations;
    private TlsSidecar tlsSidecar;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Configuration of the Topic Operator")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public EntityTopicOperatorSpec getTopicOperator() {
        return topicOperator;
    }

    public void setTopicOperator(EntityTopicOperatorSpec topicOperator) {
        this.topicOperator = topicOperator;
    }

    @Description("Configuration of the User Operator")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public EntityUserOperatorSpec getUserOperator() {
        return userOperator;
    }

    public void setUserOperator(EntityUserOperatorSpec userOperator) {
        this.userOperator = userOperator;
    }

    @Description("Pod affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("Pod's tolerations.")
    @KubeLink(group = "core", version = "v1", kind = "tolerations")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TlsSidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(TlsSidecar tlsSidecar) {
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
