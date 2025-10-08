/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user.acl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Example;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A representation of a single ACL rule for Kafka's built-in authorizer
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"type", "resource", "host", "operation", "operations"})
@EqualsAndHashCode
@ToString
public class AclRule implements UnknownPropertyPreserving {
    private AclRuleType type = AclRuleType.ALLOW;
    private AclRuleResource resource;
    private String host = "*";
    private AclOperation operation;
    private List<AclOperation> operations;
    private Map<String, Object> additionalProperties;

    public AclRule() {
    }

    public AclRule(AclRuleType type, AclRuleResource resource, String host, List<AclOperation> operations) {
        this.type = type;
        this.resource = resource;
        this.host = host;
        this.operations = operations;
    }

    @Description("The type of the rule. " +
            "ACL rules with type `allow` are used to allow user to execute the specified operations. " +
            "ACL rules with type `deny` are used to deny user to execute the specified operations. " +
            "Default value is `allow`.")
    @JsonProperty(defaultValue = "allow")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public AclRuleType getType() {
        return type;
    }

    public void setType(AclRuleType type) {
        this.type = type;
    }

    @Description("Indicates the resource for which given ACL rule applies.")
    @JsonProperty(required = true)
    public AclRuleResource getResource() {
        return resource;
    }

    public void setResource(AclRuleResource resource) {
        this.resource = resource;
    }

    @Description("The host from which the action described in the ACL rule is allowed or denied. " +
            "If not set, it defaults to `*`, allowing or denying the action from any host.")
    @JsonProperty(defaultValue = "*")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Operation which will be allowed or denied. " +
            "Supported operations are: Read, Write, Create, Delete, Alter, Describe, ClusterAction, AlterConfigs, DescribeConfigs, IdempotentWrite and All.")
    @DeprecatedProperty(movedToPath = "spec.authorization.acls[*].operations")
    @Deprecated
    @PresentInVersions("v1alpha1-v1beta2")
    public AclOperation getOperation() {
        return operation;
    }

    public void setOperation(AclOperation operation) {
        this.operation = operation;
    }

    @Description("List of operations to allow or deny. " +
            "Supported operations are: Read, Write, Create, Delete, Alter, Describe, ClusterAction, AlterConfigs, DescribeConfigs, IdempotentWrite and All. " +
            "Only certain operations work with the specified resource.")
    @Example("operations: [\"Read\",\"Write\"]")
    @RequiredInVersions("v1+")
    public List<AclOperation> getOperations() {
        return operations;
    }

    public void setOperations(List<AclOperation> operations) {
        this.operations = operations;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
