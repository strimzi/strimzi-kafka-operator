/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleClusterResource;
import io.strimzi.api.kafka.model.AclRuleGroupResource;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResource;
import io.strimzi.api.kafka.model.AclRuleTransactionalIDResource;

import kafka.security.auth.Cluster$;
import kafka.security.auth.Group$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType;
import kafka.security.auth.Topic$;
import kafka.security.auth.TransactionalId$;
import org.apache.kafka.common.resource.PatternType;

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
     * Returns the name of the resource
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type of the resource
     *
     * @return
     */
    public SimpleAclRuleResourceType getType() {
        return type;
    }

    /**
     * Returns the pattern used for this resource
     *
     * @return
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
     * Creates Kafka's Resource class from the current object
     *
     * @return
     */
    public Resource toKafkaResource()   {
        ResourceType kafkaType;
        String kafkaName;
        PatternType kafkaPattern = PatternType.LITERAL;

        switch (type) {
            case TOPIC:
                kafkaType = Topic$.MODULE$;
                kafkaName = name;

                if (AclResourcePatternType.PREFIX.equals(pattern))   {
                    kafkaPattern = PatternType.PREFIXED;
                }

                break;
            case GROUP:
                kafkaType = Group$.MODULE$;
                kafkaName = name;

                if (AclResourcePatternType.PREFIX.equals(pattern))   {
                    kafkaPattern = PatternType.PREFIXED;
                }

                break;
            case CLUSTER:
                kafkaType = Cluster$.MODULE$;
                kafkaName = "kafka-cluster";
                break;
            case TRANSACTIONAL_ID:
                kafkaType = TransactionalId$.MODULE$;
                kafkaName = name;

                if (AclResourcePatternType.PREFIX.equals(pattern))   {
                    kafkaPattern = PatternType.PREFIXED;
                }

                break;
            default:
                throw new IllegalArgumentException("Invalid Acl resource type: " + type);
        }

        return new Resource(kafkaType, kafkaName, kafkaPattern);
    }

    /**
     * Creates SimpleAclRuleResource object based on Kafka's Resource object
     *
     * @param kafkaResource Kafka's Resource object
     * @return
     */
    public static SimpleAclRuleResource fromKafkaResource(Resource kafkaResource)   {
        String resourceName;
        SimpleAclRuleResourceType resourceType;
        AclResourcePatternType resourcePattern = null;

        switch (kafkaResource.resourceType().toJava()) {
            case TOPIC:
                resourceName = kafkaResource.name();
                resourceType = SimpleAclRuleResourceType.TOPIC;

                switch (kafkaResource.patternType()) {
                    case LITERAL:
                        resourcePattern = AclResourcePatternType.LITERAL;
                        break;
                    case PREFIXED:
                        resourcePattern = AclResourcePatternType.PREFIX;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid Resource type: " + kafkaResource.resourceType());
                }

                break;
            case GROUP:
                resourceType = SimpleAclRuleResourceType.GROUP;
                resourceName = kafkaResource.name();

                switch (kafkaResource.patternType()) {
                    case LITERAL:
                        resourcePattern = AclResourcePatternType.LITERAL;
                        break;
                    case PREFIXED:
                        resourcePattern = AclResourcePatternType.PREFIX;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid Resource type: " + kafkaResource.resourceType());
                }

                break;
            case CLUSTER:
                resourceType = SimpleAclRuleResourceType.CLUSTER;
                resourceName = "kafka-cluster";
                resourcePattern = AclResourcePatternType.LITERAL;
                break;
            case TRANSACTIONAL_ID:
                resourceType = SimpleAclRuleResourceType.TRANSACTIONAL_ID;
                resourceName = kafkaResource.name();
                switch (kafkaResource.patternType()) {
                    case LITERAL:
                        resourcePattern = AclResourcePatternType.LITERAL;
                        break;
                    case PREFIXED:
                        resourcePattern = AclResourcePatternType.PREFIX;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid Resource type: " + kafkaResource.resourceType());
                }

                break;
            default:
                throw new IllegalArgumentException("Invalid Resource type: " + kafkaResource.resourceType());
        }

        return new SimpleAclRuleResource(resourceName, resourceType, resourcePattern);
    }

    /**
     * Creates SimpleAclRuleResource object based on the objects received as part fo the KAfkaUser CR
     *
     * @param resource  AclRuleResource as received in KafkaUser CR
     * @return
     */
    public static SimpleAclRuleResource fromCrd(AclRuleResource resource)   {
        if (resource instanceof AclRuleTopicResource)   {
            AclRuleTopicResource adapted = (AclRuleTopicResource) resource;
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.TOPIC, adapted.getPatternType());
        } else if (resource instanceof AclRuleGroupResource)   {
            AclRuleGroupResource adapted = (AclRuleGroupResource) resource;
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.GROUP, adapted.getPatternType());
        } else if (resource instanceof AclRuleClusterResource)   {
            return new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        } else if (resource instanceof AclRuleTransactionalIDResource)   {
            AclRuleTransactionalIDResource adapted = (AclRuleTransactionalIDResource) resource;
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.TRANSACTIONAL_ID, adapted.getPatternType());
        } else  {
            throw new IllegalArgumentException("Invalid Acl resource class: " + resource.getClass());
        }
    }
}
