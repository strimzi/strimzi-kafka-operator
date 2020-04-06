/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Toleration;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.template.EntityOperatorTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of the Entity Operator deployment.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "topicOperator", "userOperator", "affinity",
        "tolerations", "tlsSidecar"})
@EqualsAndHashCode
public class EntityOperatorSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_REPLICAS = 1;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;

    private EntityTopicOperatorSpec topicOperator;
    private EntityUserOperatorSpec userOperator;
    private Affinity affinity;
    private List<Toleration> tolerations;
    private TlsSidecar tlsSidecar;
    private EntityOperatorTemplate template;
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

    @Description("The pod's affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @DeprecatedProperty(movedToPath = "spec.template.pod.affinity")
    @Deprecated
    public Affinity getAffinity() {
        return affinity;
    }

    @Deprecated
    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("The pod's tolerations.")
    @KubeLink(group = "core", version = "v1", kind = "toleration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @DeprecatedProperty(movedToPath = "spec.template.pod.tolerations")
    @Deprecated
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    @Deprecated
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

    @Description("Template for Entity Operator resources. " +
            "The template allows users to specify how is the `Deployment` and `Pods` generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public EntityOperatorTemplate getTemplate() {
        return template;
    }

    public void setTemplate(EntityOperatorTemplate template) {
        this.template = template;
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
