/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Maven artifact represents an artifact which is downloaded from Maven repository
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "repository", "group", "artifact", "version", "insecure" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MavenArtifact extends Artifact {
    public static final String DEFAULT_REPOSITORY = "https://repo1.maven.org/maven2/";

    private String group;
    private String artifact;
    private String version;
    private String repository;
    private Boolean insecure;

    @Description("Must be `" + TYPE_MVN + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_MVN;
    }

    @Description("Maven group id. Applicable to the `maven` artifact type only.")
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Description("Maven artifact id. Applicable to the `maven` artifact type only.")
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(String artifact) {
        this.artifact = artifact;
    }

    @Description("Maven version number. Applicable to the `maven` artifact type only.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Description("Maven repository to download the artifact from. Applicable to the `maven` artifact type only.")
    @JsonProperty(defaultValue = "https://repo1.maven.org/maven2/")
    public String getRepository() {
        return repository;
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }

    @Description("By default, connections using TLS are verified to check they are secure. " +
            "The server certificate used must be valid, trusted, and contain the server name. " +
            "By setting this option to `true`, all TLS verification is disabled and the artifacts will be downloaded, even when the server is considered insecure.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getInsecure() {
        return insecure;
    }

    public void setInsecure(Boolean insecure) {
        this.insecure = insecure;
    }
}
