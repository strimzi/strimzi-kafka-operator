/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract baseclass for different representations of connector artifacts, discriminated by {@link #getType() type}.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type"
)
@JsonSubTypes(
    {
        @JsonSubTypes.Type(value = JarArtifact.class, name = Artifact.TYPE_JAR),
        @JsonSubTypes.Type(value = TgzArtifact.class, name = Artifact.TYPE_TGZ),
        @JsonSubTypes.Type(value = ZipArtifact.class, name = Artifact.TYPE_ZIP),
        @JsonSubTypes.Type(value = MavenArtifact.class, name = Artifact.TYPE_MVN),
        @JsonSubTypes.Type(value = OtherArtifact.class, name = Artifact.TYPE_OTHER)
    }
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class Artifact implements UnknownPropertyPreserving {
    public static final String TYPE_JAR = "jar";
    public static final String TYPE_TGZ = "tgz";
    public static final String TYPE_ZIP = "zip";
    public static final String TYPE_MVN = "maven";
    public static final String TYPE_OTHER = "other";
    
    private Map<String, Object> additionalProperties;

    @Description("Artifact type. " +
            "Currently, the supported artifact types are `tgz`, `jar`, `zip`, `other` and `maven`.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
