/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.entityoperator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Entity Operator deployment.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"topicOperator", "userOperator", "tlsSidecar", "template"})
@EqualsAndHashCode
@ToString
public class EntityOperatorSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private EntityTopicOperatorSpec topicOperator;
    private EntityUserOperatorSpec userOperator;
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

    @Deprecated
    @DeprecatedProperty(description = "TLS sidecar was removed in Strimzi 0.41.0. This property is ignored.")
    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TlsSidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    @Description("Template for Entity Operator resources. " +
            "The template allows users to specify how a `Deployment` and `Pod` is generated.")
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
