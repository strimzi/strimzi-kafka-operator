/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleClusterResource;
import io.strimzi.api.kafka.model.AclRuleGroupResource;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResource;
import io.strimzi.api.kafka.model.AclRuleTransactionalIdResource;

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;

/**
 * This class represents Kafka resource and is used in the SimpleAclRule objects.
 * This class is immutable.
 */
public class SimpleAclRuleResource {
    private final String name;
    private final SimpleAclRuleResourceType type;
    private final AclResourcePatternType pattern;

    /**
     * Constructor
     *
     * @param name  Name of the resource
     * @param type  Type of the resource
     * @param pattern   Pattern used to identify the resource (LITERAL or PREFIX)
     */
    public SimpleAclRuleResource(String name, SimpleAclRuleResourceType type, AclResourcePatternType pattern) {
        this.name = name;
        this.type = type;
        this.pattern = pattern;
    }

    /**
     * Returns the name of the resource.
     *
     * @return The name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type of the resource.
     *
     * @return The type.
     */
    public SimpleAclRuleResourceType getType() {
        return type;
    }

    /**
     * Returns the pattern used for this resource.
     *
     * @return The pattern.
     */
    public AclResourcePatternType getPattern() {
        return pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleAclRuleResource that = (SimpleAclRuleResource) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != that.type) return false;
        return pattern == that.pattern;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + type.hashCode();
        result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SimpleAclRuleResource(" +
                "name: " + name + ", " +
                "type: " + type + ", " +
                "pattern: " + pattern + ")";
    }

    /**
     * Creates Kafka's ResourcePattern instance from the current SimpleAclRuleResource instance
     *
     * @return the ResourcePattern instance
     */
    public ResourcePattern toKafkaResourcePattern() {
        org.apache.kafka.common.resource.ResourceType kafkaType;
        String kafkaName;
        PatternType kafkaPattern = PatternType.LITERAL;

        switch (type) {
            case TOPIC:
                kafkaType = org.apache.kafka.common.resource.ResourceType.TOPIC;
                kafkaName = name;

                if (AclResourcePatternType.PREFIX.equals(pattern))   {
                    kafkaPattern = PatternType.PREFIXED;
                }

                break;
            case GROUP:
                kafkaType = org.apache.kafka.common.resource.ResourceType.GROUP;
                kafkaName = name;

                if (AclResourcePatternType.PREFIX.equals(pattern))   {
                    kafkaPattern = PatternType.PREFIXED;
                }

                break;
            case CLUSTER:
                kafkaType = org.apache.kafka.common.resource.ResourceType.CLUSTER;
                kafkaName = "kafka-cluster";
                break;
            case TRANSACTIONAL_ID:
                kafkaType = org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
                kafkaName = name;

                if (AclResourcePatternType.PREFIX.equals(pattern))   {
                    kafkaPattern = PatternType.PREFIXED;
                }

                break;
            default:
                throw new IllegalArgumentException("Invalid Acl resource type: " + type);
        }

        if (kafkaName == null)   {
            throw new IllegalArgumentException("Name is required for resource type: " + type);
        }

        return new ResourcePattern(kafkaType, kafkaName, kafkaPattern);
    }

    /**
     * Creates SimpleAclRuleResource instance based on Kafka's ResourcePattern instance
     *
     * @param kafkaResourcePattern Kafka's ResourcePattern instance
     * @return the SimpleAclRuleResource instance
     */
    public static SimpleAclRuleResource fromKafkaResourcePattern(ResourcePattern kafkaResourcePattern) {
        String resourceName;
        SimpleAclRuleResourceType resourceType;
        AclResourcePatternType resourcePattern = null;

        switch (kafkaResourcePattern.resourceType()) {
            case TOPIC:
                resourceName = kafkaResourcePattern.name();
                resourceType = SimpleAclRuleResourceType.TOPIC;

                switch (kafkaResourcePattern.patternType()) {
                    case LITERAL:
                        resourcePattern = AclResourcePatternType.LITERAL;
                        break;
                    case PREFIXED:
                        resourcePattern = AclResourcePatternType.PREFIX;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid Resource type: " + kafkaResourcePattern.resourceType());
                }

                break;
            case GROUP:
                resourceType = SimpleAclRuleResourceType.GROUP;
                resourceName = kafkaResourcePattern.name();

                switch (kafkaResourcePattern.patternType()) {
                    case LITERAL:
                        resourcePattern = AclResourcePatternType.LITERAL;
                        break;
                    case PREFIXED:
                        resourcePattern = AclResourcePatternType.PREFIX;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid Resource type: " + kafkaResourcePattern.resourceType());
                }

                break;
            case CLUSTER:
                resourceType = SimpleAclRuleResourceType.CLUSTER;
                resourceName = "kafka-cluster";
                resourcePattern = AclResourcePatternType.LITERAL;
                break;
            case TRANSACTIONAL_ID:
                resourceType = SimpleAclRuleResourceType.TRANSACTIONAL_ID;
                resourceName = kafkaResourcePattern.name();
                switch (kafkaResourcePattern.patternType()) {
                    case LITERAL:
                        resourcePattern = AclResourcePatternType.LITERAL;
                        break;
                    case PREFIXED:
                        resourcePattern = AclResourcePatternType.PREFIX;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid Resource type: " + kafkaResourcePattern.resourceType());
                }

                break;
            default:
                throw new IllegalArgumentException("Invalid Resource type: " + kafkaResourcePattern.resourceType());
        }

        return new SimpleAclRuleResource(resourceName, resourceType, resourcePattern);
    }

    /**
     * Creates SimpleAclRuleResource object based on the objects received as part fo the KafkaUser CR
     *
     * @param resource  AclRuleResource as received in KafkaUser CR
     * @return The resource.
     */
    public static SimpleAclRuleResource fromCrd(AclRuleResource resource)   {
        if (resource instanceof AclRuleTopicResource adapted)   {
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.TOPIC, adapted.getPatternType());
        } else if (resource instanceof AclRuleGroupResource adapted)   {
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.GROUP, adapted.getPatternType());
        } else if (resource instanceof AclRuleClusterResource)   {
            return new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        } else if (resource instanceof AclRuleTransactionalIdResource adapted)   {
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.TRANSACTIONAL_ID, adapted.getPatternType());
        } else  {
            throw new IllegalArgumentException("Invalid Acl resource class: " + resource.getClass());
        }
    }
}
