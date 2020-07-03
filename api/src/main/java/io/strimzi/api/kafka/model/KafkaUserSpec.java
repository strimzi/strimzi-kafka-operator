/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.template.KafkaUserTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "authentication", "authorization", "quotas" })
@EqualsAndHashCode
public class KafkaUserSpec  implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private KafkaUserAuthentication authentication;
    private KafkaUserAuthorization authorization;
    private KafkaUserQuotas quotas;
    private KafkaUserTemplate template;
    private Map<String, Object> additionalProperties;

    @Description("Authentication mechanism enabled for this Kafka user.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("Authorization rules for this Kafka user.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(KafkaUserAuthorization authorization) {
        this.authorization = authorization;
    }

    @Description("Quotas on requests to control the broker resources used by clients. " +
            "Network bandwidth and request rate quotas can be enforced." +
            "Kafka documentation for Kafka User quotas can be found at http://kafka.apache.org/documentation/#design_quotas.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserQuotas getQuotas() {
        return quotas;
    }

    public void setQuotas(KafkaUserQuotas kafkaUserQuotas) {
        this.quotas = kafkaUserQuotas;
    }

    @Description("Template to specify how Kafka User `Secrets` are generated.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaUserTemplate template) {
        this.template = template;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
