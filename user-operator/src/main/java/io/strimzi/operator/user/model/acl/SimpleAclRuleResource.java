package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleGroupResource;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResource;

import kafka.security.auth.Cluster$;
import kafka.security.auth.Group$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType;
import kafka.security.auth.Topic$;
import org.apache.kafka.common.resource.PatternType;

public class SimpleAclRuleResource {
    private final String name;
    private final SimpleAclRuleResourceType type;
    private final AclResourcePatternType pattern;

    public SimpleAclRuleResource(String name, SimpleAclRuleResourceType type, AclResourcePatternType pattern) {
        this.name = name;
        this.type = type;
        this.pattern = pattern;
    }

    public String getName() {
        return name;
    }

    public SimpleAclRuleResourceType getType() {
        return type;
    }

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

    public Resource toKafkaResource()   {
        ResourceType kafkaType;
        String kafkaName = "";
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
                break;
            default:
                throw new IllegalArgumentException("Invalid Acl resource type: " + type);
        }

        return new Resource(kafkaType, kafkaName, kafkaPattern);
    }

    public static SimpleAclRuleResource fromKafkaResource(Resource kafkaResource)   {
        String resourceName = null;
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
                break;
            default:
                throw new IllegalArgumentException("Invalid Resource type: " + kafkaResource.resourceType());
        }

        return new SimpleAclRuleResource(resourceName, resourceType, resourcePattern);
    }

    public static SimpleAclRuleResource fromCrd(AclRuleResource resource)   {
        if (resource instanceof AclRuleTopicResource)   {
            AclRuleTopicResource adapted = (AclRuleTopicResource) resource;
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.TOPIC, adapted.getPatternType());
        } else if (resource instanceof AclRuleGroupResource)   {
            AclRuleGroupResource adapted = (AclRuleGroupResource) resource;
            return new SimpleAclRuleResource(adapted.getName(), SimpleAclRuleResourceType.GROUP, adapted.getPatternType());
        } else if (resource instanceof AclRuleTopicResource)   {
            return new SimpleAclRuleResource(null, SimpleAclRuleResourceType.CLUSTER, null);
        } else  {
            throw new IllegalArgumentException("Invalid Acl resource class: " + resource.getClass());
        }
    }
}
