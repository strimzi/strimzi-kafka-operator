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

import java.util.List;

/**
 * Represents Docker output from the build
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "image", "pushSecret", "additionalKanikoOptions", "type" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DockerOutput extends Output {
    public static final String ALLOWED_KANIKO_OPTIONS = "--customPlatform, --insecure, --insecure-pull, " +
            "--insecure-registry, --log-format, --log-timestamp, --registry-mirror, --reproducible, --single-snapshot, " +
            "--skip-tls-verify, --skip-tls-verify-pull, --skip-tls-verify-registry, --verbosity, --snapshotMode, " +
            "--use-new-run";

    private String pushSecret;
    private List<String> additionalKanikoOptions;

    @Description("Must be `" + TYPE_DOCKER + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_DOCKER;
    }

    @Description("The full name which should be used for tagging and pushing the newly built image. " +
            "For example `quay.io/my-organization/my-custom-connect:latest`. " +
            "Required")
    @JsonProperty(required = true)
    public String getImage() {
        return super.getImage();
    }

    public void setImage(String image) {
        super.setImage(image);
    }

    @Description("Container Registry Secret with the credentials for pushing the newly built image.")
    public String getPushSecret() {
        return pushSecret;
    }

    public void setPushSecret(String pushSecret) {
        this.pushSecret = pushSecret;
    }

    @Description("Configures additional options which will be passed to the Kaniko executor when building the new Connect image. " +
            "Allowed options are: " + ALLOWED_KANIKO_OPTIONS + ". " +
            "These options will be used only on Kubernetes where the Kaniko executor is used. " +
            "They will be ignored on OpenShift. " +
            "The options are described in the link:https://github.com/GoogleContainerTools/kaniko[Kaniko GitHub repository^]. " +
            "Changing this field does not trigger new build of the Kafka Connect image.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAdditionalKanikoOptions() {
        return additionalKanikoOptions;
    }

    public void setAdditionalKanikoOptions(List<String> additionalKanikoOptions) {
        this.additionalKanikoOptions = additionalKanikoOptions;
    }
}
