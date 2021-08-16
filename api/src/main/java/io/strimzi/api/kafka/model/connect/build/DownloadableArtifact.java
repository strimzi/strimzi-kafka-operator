/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Artifact which can be just downloaded from an URL
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public abstract class DownloadableArtifact extends Artifact {
    private static final long serialVersionUID = 1L;

    private String url;
    private String sha512sum;

    @Description("URL of the artifact which will be downloaded. " +
            "Strimzi does not do any security scanning of the downloaded artifacts. " +
            "For security reasons, you should first verify the artifacts manually and configure the checksum verification to make sure the same artifact is used in the automated build. " +
            "Required." +
            "Not applicable to the `maven` artifact type.")
    @Pattern("^(https?|ftp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]")
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Description("SHA512 checksum of the artifact. " +
            "Optional. " +
            "If specified, the checksum will be verified while building the new container. " +
            "If not specified, the downloaded artifact will not be verified. Not applicable for 'maven' type of artifact." +
            "Not applicable to the `maven` artifact type.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getSha512sum() {
        return sha512sum;
    }

    public void setSha512sum(String sha512sum) {
        this.sha512sum = sha512sum;
    }
}
