/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;
import lombok.EqualsAndHashCode;

/**
 * Maven artifact represents an artifact which is downloaded from Maven repository
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "repository", "group", "artifact", "version" })
@EqualsAndHashCode
public class MavenArtifact extends Artifact {
    private static final long serialVersionUID = 1L;
    public static final String DEFAULT_REPOSITORY = "https://repo1.maven.org/maven2/";

    private String group;
    private String artifact;
    private String version;
    private String repository;

    @Description("Must be `" + TYPE_MVN + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_MVN;
    }

    @Description("Group of the artifact. Applicable for 'maven' type of artifact only.")
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Description("Name of the artifact. Applicable for 'maven' type of artifact only.")
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(String artifact) {
        this.artifact = artifact;
    }

    @Description("Version of the artifact. Applicable for 'maven' type of artifact only.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Description("The repository to download the artifact from. Applicable for 'maven' type of artifact only.")
    @DefaultValue("https://repo1.maven.org/maven2/")
    public String getRepository() {
        return repository;
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }
}
