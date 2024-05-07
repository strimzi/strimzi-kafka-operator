/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * JAR artifact represents an artifact which is simply downloaded
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "url", "sha512sum", "insecure" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class JarArtifact extends DownloadableArtifact {
    @Description("Must be `" + TYPE_JAR + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_JAR;
    }
}
