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
@JsonPropertyOrder({ "image", "pushSecret", "additionalKanikoOptions", "additionalBuildahBuildOptions", "additionalBuildahPushOptions", "type" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DockerOutput extends Output {
    // Note: --customPlatform is deprecated option replaced with --custom-platform. We enable both for backwards compatibility
    public static final String ALLOWED_KANIKO_OPTIONS = "--customPlatform, --custom-platform, --insecure, --insecure-pull, " +
            "--insecure-registry, --log-format, --log-timestamp, --registry-mirror, --reproducible, --single-snapshot, " +
            "--skip-tls-verify, --skip-tls-verify-pull, --skip-tls-verify-registry, --verbosity, --snapshotMode, " +
            "--use-new-run, --registry-certificate, --registry-client-cert, --ignore-path";

    public static final String ALLOWED_BUILDAH_BUILD_OPTIONS = "--annotation, --authfile, --cert-dir, --creds, --decryption-key, " +
        "--env, --label, --logfile, --manifest, --retry-delay, --secret, --security-opt, --timestamp, --tls-verify";
    public static final String ALLOWED_BUILDAH_PUSH_OPTIONS = "--authfile, --cert-dir, --creds, --format, --quiet, --remove-signatures, " +
        "--retry, --retry-delay, --sign-by, --tls-verify";

    private String pushSecret;
    private List<String> additionalKanikoOptions;
    private List<String> additionalBuildahBuildOptions;
    private List<String> additionalBuildahPushOptions;

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

    @Description("Configures additional options which will be passed to the Buildah `build` when building the new Connect image. " +
        "Allowed options are: " + ALLOWED_BUILDAH_BUILD_OPTIONS + ". " +
        "Those will be used only on Kubernetes, where is Buildah used. " +
        "They will be ignored on OpenShift. " +
        "The options are described in the link:https://github.com/containers/buildah/blob/main/docs/buildah-build.1.md[Buildah build document^]. " +
        "Changing this field does not trigger new build of the Kafka Connect image."
    )
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAdditionalBuildahBuildOptions() {
        return additionalBuildahBuildOptions;
    }

    public void setAdditionalBuildahBuildOptions(List<String> additionalBuildahBuildOptions) {
        this.additionalBuildahBuildOptions = additionalBuildahBuildOptions;
    }

    @Description("Configures additional options which will be passed to the Buildah `push` when pushing the new Connect image. " +
        "Allowed options are: " + ALLOWED_BUILDAH_PUSH_OPTIONS + ". " +
        "Those will be used only on Kubernetes, where is Buildah used. " +
        "They will be ignored on OpenShift. " +
        "The options are described in the link:https://github.com/containers/buildah/blob/main/docs/buildah-push.1.md[Buildah push document^]. " +
        "Changing this field does not trigger new build of the Kafka Connect image."
    )
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAdditionalBuildahPushOptions() {
        return additionalBuildahPushOptions;
    }

    public void setAdditionalBuildahPushOptions(List<String> additionalBuildahPushOptions) {
        this.additionalBuildahPushOptions = additionalBuildahPushOptions;
    }
}
