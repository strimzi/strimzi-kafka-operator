/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Image Volume artifact represents an artifact which is mounted as a Kubernetes Image Volume
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "reference", "pullPolicy" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ImageArtifact extends MountedArtifact {
    private String reference;
    private String pullPolicy;

    @Description("Must be `" + TYPE_IMAGE + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_IMAGE;
    }

    @Description("Reference to the container image (OCI artifact) containing the Kafka Connect plugin. " +
            "The image is mounted as a volume and provides the plugin binary. " +
            "Required.")
    @JsonProperty(required = true)
    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    @Description("Policy that determines when the container image (OCI artifact) is pulled.\n\n" +
            "Possible values are:\n\n" +
            "* `Always`: Always pull the image. If the pull fails, container creation fails.\n" +
            "* `Never`: Never pull the image. Use only a locally available image. Container creation fails if the image isn’t present.\n" +
            "* `IfNotPresent`: Pull the image only if it’s not already available locally. Container creation fails if the image isn’t present and the pull fails.\n\n" +
            "Defaults to `Always` if `:latest` tag is specified, or `IfNotPresent` otherwise.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getPullPolicy() {
        return pullPolicy;
    }

    public void setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
    }
}
