/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Represents Docker output from the build
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "image", "pushSecret" })
@EqualsAndHashCode
public class DockerOutput extends Output {
    private static final long serialVersionUID = 1L;

    private String image;
    private String pushSecret;
    private boolean insecure = false;

    @Description("Must be `" + TYPE_DOCKER + "`")
    @Override
    public String getType() {
        return TYPE_DOCKER;
    }

    @Description("Docker Registry Secret with the credentials for pushing the newly built image. " +
            "Required")
    @JsonProperty(required = true)
    public String getPushSecret() {
        return pushSecret;
    }

    public void setPushSecret(String pushSecret) {
        this.pushSecret = pushSecret;
    }

    @Description("When true, the target repository will be treated as insecure.")
    public boolean isInsecure() {
        return insecure;
    }

    public void setInsecure(boolean insecure) {
        this.insecure = insecure;
    }
}
