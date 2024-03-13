/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaUserTlsExternalClientAuthentication extends KafkaUserAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_TLS_EXTERNAL = "tls-external";

    @Description("Must be `" + TYPE_TLS_EXTERNAL + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_TLS_EXTERNAL;
    }


}
