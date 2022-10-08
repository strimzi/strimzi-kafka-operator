/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A representation of a single ACL rule for AclAuthorizer
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@EqualsAndHashCode
public class AclRule implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private AclRuleType type = AclRuleType.ALLOW;
    private AclRuleResource resource;
    private String host = "*";
    private AclOperation operation;
    private List<AclOperation> operations;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    public AclRule() {
    }

    public AclRule(AclRuleType type, AclRuleResource resource, String host, List<AclOperation> operations) {
        this.type = type;
        this.resource = resource;
        this.host = host;
        this.operations = operations;
    }

    @Description("The type of the rule. " +
            "Currently the only supported type is `allow`. " +
            "ACL rules with type `allow` are used to allow user to execute the specified operations. " +
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

    @Description("The host from which the action described in the ACL rule is allowed or denied.")
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
    public AclOperation getOperation() {
        return operation;
    }

    public void setOperation(AclOperation operation) {
        this.operation = operation;
    }

    @Description("List of operations which will be allowed or denied. " +
            "Supported operations are: Read, Write, Create, Delete, Alter, Describe, ClusterAction, AlterConfigs, DescribeConfigs, IdempotentWrite and All.")
    @Example("operations: [\"Read\",\"Write\"]")
    public List<AclOperation> getOperations() {
        return operations;
    }

    public void setOperations(List<AclOperation> operations) {
        this.operations = operations;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
