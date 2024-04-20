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
 * Other artifact represents an artifact of assorted types. Users can specify a name of the file under which it will be
 * stored in the container image.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "url", "sha512sum", "fileName", "insecure" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class OtherArtifact extends DownloadableArtifact {
    String fileName;

    @Description("Must be `" + TYPE_OTHER + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_OTHER;
    }

    @Description("Name under which the artifact will be stored.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
