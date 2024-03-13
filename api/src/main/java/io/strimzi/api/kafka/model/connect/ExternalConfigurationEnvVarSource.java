/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.SecretKeySelector;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation for environment variables which will be passed to Kafka Connect
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"secretKeyRef", "configMapKeyRef"})
@EqualsAndHashCode
@ToString
public class ExternalConfigurationEnvVarSource implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private SecretKeySelector secretKeyRef;
    private ConfigMapKeySelector configMapKeyRef;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    // TODO: We should make it possible to generate a CRD configuring that exactly one of secretKeyRef and configMapKeyRef has to be defined.

    @Description("Reference to a key in a Secret.")
    @KubeLink(group = "core", version = "v1", kind = "secretkeyselector")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public SecretKeySelector getSecretKeyRef() {
        return secretKeyRef;
    }

    public void setSecretKeyRef(SecretKeySelector secretKeyRef) {
        this.secretKeyRef = secretKeyRef;
    }

    @Description("Reference to a key in a ConfigMap.")
    @KubeLink(group = "core", version = "v1", kind = "configmapkeyselector")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public ConfigMapKeySelector getConfigMapKeyRef() {
        return configMapKeyRef;
    }

    public void setConfigMapKeyRef(ConfigMapKeySelector configMapKeyRef) {
        this.configMapKeyRef = configMapKeyRef;
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

