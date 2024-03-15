/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.jmx;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@DescriptionFile 
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"authentication"})
@EqualsAndHashCode
@ToString
public class KafkaJmxOptions implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaJmxAuthentication authentication;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Authentication configuration for connecting to the JMX port")
    @JsonProperty("authentication")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaJmxAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaJmxAuthentication authentication) {
        this.authentication = authentication;
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
