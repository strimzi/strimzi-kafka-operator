/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "validityDays", "renewalDays"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaUserTlsClientAuthentication extends KafkaUserAuthentication {
    public static final String TYPE_TLS = "tls";

    private Integer validityDays;
    private Integer renewalDays;

    @Description("Must be `" + TYPE_TLS + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_TLS;
    }

    @Description(
        "Number of days for which the user certificate is valid. " +
        "Both this property and `renewalDays` must be configured together." +
        "The value must be greater than 0 and greater than `renewalDays`." +
        "If not configured, the Clients CA configuration is used."
    )
    @Minimum(1)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Integer getValidityDays() {
        return this.validityDays;
    }

    public void setValidityDays(Integer validityDays) {
        this.validityDays = validityDays;
    }

    @Description(
        "Number of days before certificate expiration when the user certificate is renewed. " +
        "Both this property and `validityDays` must be configured together." +
        "The value must be greater than 0 and less than `validityDays`." +
        "If not configured, the Clients CA configuration is used."
    )
    @Minimum(1)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Integer getRenewalDays() {
        return renewalDays;
    }

    public void setRenewalDays(Integer renewalDays) {
        this.renewalDays = renewalDays;
    }

}
