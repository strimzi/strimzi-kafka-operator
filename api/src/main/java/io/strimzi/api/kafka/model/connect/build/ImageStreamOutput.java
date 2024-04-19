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

/**
 * Represents Docker output from the build
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "image" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ImageStreamOutput extends Output {
    @Description("Must be `" + TYPE_IMAGESTREAM + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_IMAGESTREAM;
    }

    @Description("The name and tag of the ImageStream where the newly built image will be pushed. " +
            "For example `my-custom-connect:latest`. " +
            "Required")
    @JsonProperty(required = true)
    @Override
    public String getImage() {
        return super.getImage();
    }
}
