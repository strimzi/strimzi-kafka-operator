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
@JsonPropertyOrder({ "image", "pushSecret", "additionalBuildOptions", "additionalPushOptions", "type" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DockerOutput extends Output {
    // Note: --customPlatform is deprecated option replaced with --custom-platform. We enable both for backwards compatibility
    public static final String ALLOWED_KANIKO_OPTIONS = "--customPlatform, --custom-platform, --insecure, --insecure-pull, " +
            "--insecure-registry, --log-format, --log-timestamp, --registry-mirror, --reproducible, --single-snapshot, " +
            "--skip-tls-verify, --skip-tls-verify-pull, --skip-tls-verify-registry, --verbosity, --snapshotMode, " +
            "--use-new-run, --registry-certificate, --registry-client-cert, --ignore-path";

    public static final String ALLOWED_BUILDAH_BUILD_OPTIONS = "--authfile, --cert-dir, --creds, --decryption-key, --retry, --retry-delay, --tls-verify";
    public static final String ALLOWED_BUILDAH_PUSH_OPTIONS = "--authfile, --cert-dir, --creds, --quiet, --retry, --retry-delay, --tls-verify";

    private String pushSecret;
    private List<String> additionalBuildOptions;
    private List<String> additionalPushOptions;

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

    @Description("Configures additional options to pass to the `build` command of either Kaniko or Buildah (depending on the feature gate setting) when building a new Kafka Connect image. " +
        "Allowed Kaniko options: " + ALLOWED_KANIKO_OPTIONS + ". " +
        "Allowed Buildah `build` options: " + ALLOWED_BUILDAH_BUILD_OPTIONS + ". " +
        "Those options are used only on Kubernetes, where Kaniko and Buildah are available. " +
        "They are ignored on OpenShift. " +
        "For more information, see the link:https://github.com/GoogleContainerTools/kaniko[Kaniko GitHub repository^] or the link:https://github.com/containers/buildah/blob/main/docs/buildah-build.1.md[Buildah build document^]. " +
        "Changing this field does not trigger a rebuild of the Kafka Connect image."
    )
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAdditionalBuildOptions() {
        return additionalBuildOptions;
    }

    public void setAdditionalBuildOptions(List<String> additionalBuildOptions) {
        this.additionalBuildOptions = additionalBuildOptions;
    }

    @Description("Configures additional options to pass to the Buildah `push` command when pushing a new Connect image. " +
        "Allowed options: " + ALLOWED_BUILDAH_PUSH_OPTIONS + ". " +
        "Those options are used only on Kubernetes, where Buildah is available. " +
        "They are ignored on OpenShift. " +
        "For more information, see the link:https://github.com/containers/buildah/blob/main/docs/buildah-push.1.md[Buildah push document^]. " +
        "Changing this field does not trigger a rebuild of the Kafka Connect image."
    )
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAdditionalPushOptions() {
        return additionalPushOptions;
    }

    public void setAdditionalPushOptions(List<String> additionalPushOptions) {
        this.additionalPushOptions = additionalPushOptions;
    }
}
