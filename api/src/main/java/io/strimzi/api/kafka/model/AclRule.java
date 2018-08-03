/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;

/**
 * A representation of a single ACL rule for SimpleAclAuthorizer
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AclRule implements Serializable {
    private static final long serialVersionUID = 1L;

    private AclRuleType type;
    private AclRuleResource resource;
    private String host;
    private String operation;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The type of the rule." +
            "Currently the only supported type is `allow`." +
            "ACL rules with type `allow` are used to allow user to execute the specified operations. " +
            "Default value is `allow`.")
    @DefaultValue("allow")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
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
    @DefaultValue("*")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Operation which will be allowed or denied. Supported operations are: Read, Write, Create, Delete, Alter, Describe, ClusterAction and All.")
    @JsonProperty(required = true)
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}

