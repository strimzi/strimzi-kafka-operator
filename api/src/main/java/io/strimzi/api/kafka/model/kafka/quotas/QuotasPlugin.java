/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.quotas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = QuotasPluginKafka.TYPE_KAFKA, value = QuotasPluginKafka.class),
    @JsonSubTypes.Type(name = QuotasPluginStrimzi.TYPE_STRIMZI, value = QuotasPluginStrimzi.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class QuotasPlugin implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Quotas plugin type. " +
        "Currently, the supported types are `kafka` and `strimzi`. " +
        "`kafka` quotas type uses Kafka's built-in quotas plugin. " +
        "`strimzi` quotas type uses Strimzi quotas plugin.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
