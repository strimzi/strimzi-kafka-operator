/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user.acl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A representation of a topic resource for ACLs
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"type", "name", "patternType"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class AclRuleTopicResource extends AclRuleResource {
    public static final String TYPE_TOPIC = "topic";

    private String name;
    private AclResourcePatternType patternType = AclResourcePatternType.LITERAL;

    @Description("Must be `" + TYPE_TOPIC + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_TOPIC;
    }

    @Description("Describes the pattern used in the resource field. " +
            "The supported types are `literal` and `prefix`. " +
            "With `literal` pattern type, the resource field will be used as a definition of a full topic name. " +
            "With `prefix` pattern type, the resource name will be used only as a prefix. " +
            "Default value is `literal`.")
    @JsonProperty(defaultValue = "literal")
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    public AclResourcePatternType getPatternType() {
        return patternType;
    }

    public void setPatternType(AclResourcePatternType patternType) {
        this.patternType = patternType;
    }

    @Description("Name of resource for which given ACL rule applies. " +
            "Can be combined with `patternType` field to use prefix pattern.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}