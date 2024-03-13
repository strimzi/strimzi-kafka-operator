/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.api.kafka.model.kafka.tieredstorage;

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

/**
 * Abstract baseclass for different representations of tieredStorage, discriminated by {@link #getType() type}.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TieredStorageCustom.class, name = TieredStorage.TYPE_CUSTOM)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class TieredStorage implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;
    public static final String TYPE_CUSTOM = "custom";

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Storage type, only 'custom' is supported at the moment.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}

