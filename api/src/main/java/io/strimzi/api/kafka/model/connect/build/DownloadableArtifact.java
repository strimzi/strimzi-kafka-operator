/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Artifact which can be just downloaded from an URL
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public abstract class DownloadableArtifact extends Artifact {
    private String url;
    private String sha512sum;
    private Boolean insecure;

    @Description("URL of the artifact which will be downloaded. " +
            "Strimzi does not do any security scanning of the downloaded artifacts. " +
            "For security reasons, you should first verify the artifacts manually and configure the checksum verification to make sure the same artifact is used in the automated build. " +
            "Required for `jar`, `zip`, `tgz` and `other` artifacts. " +
            "Not applicable to the `maven` artifact type.")
    @Pattern("^(https?|ftp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$")
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Description("SHA512 checksum of the artifact. " +
            "Optional. " +
            "If specified, the checksum will be verified while building the new container. " +
            "If not specified, the downloaded artifact will not be verified. " +
            "Not applicable to the `maven` artifact type. ")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getSha512sum() {
        return sha512sum;
    }

    public void setSha512sum(String sha512sum) {
        this.sha512sum = sha512sum;
    }

    @Description("By default, connections using TLS are verified to check they are secure. " +
            "The server certificate used must be valid, trusted, and contain the server name. " +
            "By setting this option to `true`, all TLS verification is disabled and the artifact will be downloaded, even when the server is considered insecure.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getInsecure() {
        return insecure;
    }

    public void setInsecure(Boolean insecure) {
        this.insecure = insecure;
    }
}
