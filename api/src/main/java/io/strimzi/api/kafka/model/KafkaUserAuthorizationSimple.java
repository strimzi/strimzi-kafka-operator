/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the broker authorization
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "acls"})
@EqualsAndHashCode
public class KafkaUserAuthorizationSimple extends KafkaUserAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SIMPLE = "simple";

    private List<AclRule> acls;

    @Description("Must be `" + TYPE_SIMPLE + "`")
    @Override
    public String getType() {
        return TYPE_SIMPLE;
    }

    @Description("List of ACL rules which should be applied to this user.")
    @JsonProperty(required = true)
    public List<AclRule> getAcls() {
        return acls;
    }

    public void setAcls(List<AclRule> acls) {
        this.acls = acls;
    }
}
