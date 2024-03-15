/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "authentication", "authorization", "quotas", "template" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaUserSpec extends Spec {
    private static final long serialVersionUID = 1L;

    private KafkaUserAuthentication authentication;
    private KafkaUserAuthorization authorization;
    private KafkaUserQuotas quotas;
    private KafkaUserTemplate template;

    @Description("Authentication mechanism enabled for this Kafka user. " +
            "The supported authentication mechanisms are `scram-sha-512`, `tls`, and `tls-external`. \n\n" +
            "* `scram-sha-512` generates a secret with SASL SCRAM-SHA-512 credentials.\n" +
            "* `tls` generates a secret with user certificate for mutual TLS authentication.\n" +
            "* `tls-external` does not generate a user certificate. " +
            "  But prepares the user for using mutual TLS authentication using a user certificate generated outside the User Operator.\n" +
            "  ACLs and quotas set for this user are configured in the `CN=<username>` format.\n\n" +
            "Authentication is optional. " +
            "If authentication is not configured, no credentials are generated. " +
            "ACLs and quotas set for the user are configured in the `<username>` format suitable for SASL authentication.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("Authorization rules for this Kafka user.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(KafkaUserAuthorization authorization) {
        this.authorization = authorization;
    }

    @Description("Quotas on requests to control the broker resources used by clients. " +
            "Network bandwidth and request rate quotas can be enforced." +
            "Kafka documentation for Kafka User quotas can be found at http://kafka.apache.org/documentation/#design_quotas.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserQuotas getQuotas() {
        return quotas;
    }

    public void setQuotas(KafkaUserQuotas kafkaUserQuotas) {
        this.quotas = kafkaUserQuotas;
    }

    @Description("Template to specify how Kafka User `Secrets` are generated.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaUserTemplate template) {
        this.template = template;
    }
}
