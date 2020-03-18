/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaUserScramSha512ClientAuthentication extends KafkaUserAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SCRAM_SHA_512 = "scram-sha-512";

    @Description("Must be `" + TYPE_SCRAM_SHA_512 + "`")
    @Override
    public String getType() {
        return TYPE_SCRAM_SHA_512;
    }


}
