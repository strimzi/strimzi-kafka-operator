/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
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
    private String topic;
    private TopicPatternType topicPatternType;
    private String host;
    private String group;
    private String operation;
    private Boolean consumer;
    private Boolean producer;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The type of the rule." +
            "Currently the only supported type is `allow`." +
            "ACL rules with type `allow` are used to allow user to execute the specified operations.")
    @DefaultValue("allow")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public AclRuleType getType() {
        return type;
    }

    public void setType(AclRuleType type) {
        this.type = type;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Description("Describes the pattern used in the `topic` field. The supported types are `literal` and `prefix`. With `literal` pattern type, the `topic` will be used as a definition of a full topic name")
    @DefaultValue("literal")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public TopicPatternType getTopicPatternType() {
        return topicPatternType;
    }

    public void setTopicPatternType(TopicPatternType topicPatternType) {
        this.topicPatternType = topicPatternType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Boolean getConsumer() {
        return consumer;
    }

    public void setConsumer(Boolean consumer) {
        this.consumer = consumer;
    }

    public Boolean getProducer() {
        return producer;
    }

    public void setProducer(Boolean producer) {
        this.producer = producer;
    }

    @Description("The initial delay before first the health is first checked.")
    @Minimum(0)
    @DefaultValue("15")
    public int getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public void setInitialDelaySeconds(int initialDelaySeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
    }

    @Description("The timeout for each attempted health check.")
    @Minimum(0)
    @DefaultValue("5")
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
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

enum AclRuleType {
    ALLOW,
    DENY;

    @JsonCreator
    public static AclRuleType forValue(String value) {
        switch (value) {
            case "deny":
                return ALLOW;
            case "allow":
                return DENY;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case ALLOW:
                return "allow";
            case DENY:
                return "deny";
            default:
                return null;
        }
    }
}

enum TopicPatternType {
    LITERAL,
    PREFIX;

    @JsonCreator
    public static TopicPatternType forValue(String value) {
        switch (value) {
            case "literal":
                return LITERAL;
            case "prefix":
                return PREFIX;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case LITERAL:
                return "literal";
            case PREFIX:
                return "prefix";
            default:
                return null;
        }
    }
}