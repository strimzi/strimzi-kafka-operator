/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.CelValidation;
import io.strimzi.crdgenerator.annotations.Description;
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
@CelValidation(rules = {
    @CelValidation.CelValidationRule(
        rule = "has(self.renewalDays) == has(self.validityDays)",
        message = "Both 'validityDays' and 'renewalDays' must be set together, or both must be unset."
        ),
    @CelValidation.CelValidationRule(
        rule = "!has(self.renewalDays) || !has(self.validityDays) || self.renewalDays < self.validityDays",
        message = "'renewalDays' must be less than 'validityDays'."
        )
})
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

    @CelValidation(rules = {
        @CelValidation.CelValidationRule(
            rule = "self > 0",
            message = "'validityDays' has to be higher than 0."
            )
    })
    @Description(
        "Number of days for which the user certificate should be valid. " +
        "If not configured, Clients CA configuration is used."
    )
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Integer getValidityDays() {
        return this.validityDays;
    }

    public void setValidityDays(Integer validityDays) {
        this.validityDays = validityDays;
    }

    @CelValidation(rules = {
        @CelValidation.CelValidationRule(
            rule = "self > 0",
            message = "'renewalDays' has to be higher than 0."
            )
    })
    @Description(
        "Configures how many days before the certificate expiration should be the user certificate renewed. " +
        "If not configured, Clients CA configuration is used."
    )
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Integer getRenewalDays() {
        return renewalDays;
    }

    public void setRenewalDays(Integer renewalDays) {
        this.renewalDays = renewalDays;
    }

}
