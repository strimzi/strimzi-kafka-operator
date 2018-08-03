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
    private AclResourcePatternType resourcePatternType;
    private String topic;
    private String group;
    private Boolean cluster;
    private String host;
    private String operation;
    private Boolean consumer;
    private Boolean producer;
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

    @Description("Describes the pattern used in the resource fields. " +
            "Resource fields are the fields `cluster`, `topic` and `group`. " +
            "The supported types are `literal` and `prefix`. " +
            "With `literal` pattern type, the resource field will be used as a definition of a full topic name. " +
            "With `prefix` pattern type, the resoruce name will be used only as a prefix. " +
            "Default value is `literal`.")
    @DefaultValue("literal")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public AclResourcePatternType getResourcePatternType() {
        return resourcePatternType;
    }

    public void setResourcePatternType(AclResourcePatternType resourcePatternType) {
        this.resourcePatternType = resourcePatternType;
    }

    @Description("Indicates the topic resource for which given ACL rule applies. " +
            "Can be combined with `resourceType` field to use prefix pattern.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Description("Indicates the consumer group resource for which given ACL rule applies. " +
            "Can be combined with `resourceType` field to use prefix pattern.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Description("Indicates the that the ACL rule applies to the cluster resource")
    @DefaultValue("false")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Boolean isCluster() {
        return cluster;
    }

    public void setCluster(Boolean cluster) {
        this.cluster = cluster;
    }

    @Description("The host from which the action described in the ACL rule is allowed or denied.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Operation which will be allowed or denied. Supported operations are: Read, Write, Create, Delete, Alter, Describe, ClusterAction and All.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    @Description("Shortcut option to add all acl rules required for producer role. " +
            "This will generate acl rules that allow WRITE, DESCRIBE and CREATE on topic.")
    @DefaultValue("false")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Boolean isConsumer() {
        return consumer;
    }

    public void setConsumer(Boolean consumer) {
        this.consumer = consumer;
    }

    @Description("Shortcut option to add all acl rules required for consumer role. " +
            "This will generate acl rules that allow READ, DESCRIBE on topic and READ on consumer-group.")
    @DefaultValue("false")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Boolean isProducer() {
        return producer;
    }

    public void setProducer(Boolean producer) {
        this.producer = producer;
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

