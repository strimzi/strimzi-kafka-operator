/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.api.kafka.model.user.acl.AclRuleType;
import io.strimzi.api.kafka.model.user.acl.StrimziAclOperation;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable class which represents a single ACL rule for Kafka's built-in authorizer.
 * The main reason for not using directly the classes from the api module is that we need immutable objects for use in Sets.
 */
public class SimpleAclRule {
    private final AclRuleType type;
    private final SimpleAclRuleResource resource;
    private final String host;
    private final StrimziAclOperation operation;

    /**
     * Constructor
     *
     * @param type      Type of the Acl rule (Allow or Deny)
     * @param resource  The resource to which this rule applies (Topic, Group, Cluster, ...)
     * @param host      The host from which is this rule allowed / denied
     * @param operation The Operation which is allowed or denied
     */
    public SimpleAclRule(AclRuleType type, SimpleAclRuleResource resource, String host, StrimziAclOperation operation) {
        this.type = type;
        this.resource = resource;
        this.host = host;
        this.operation = operation;
    }

    /**
     * Returns the type of the ACL rule.
     *
     * @return The type.
     */
    public AclRuleType getType() {
        return type;
    }

    /**
     * Returns the resource to which this rule applies.
     *
     * @return The resource.
     */
    public SimpleAclRuleResource getResource() {
        return resource;
    }

    /**
     * Returns the host from which this rule is allowed / denied.
     *
     * @return The host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the operation which is allowed / denied.
     *
     * @return The operation.
     */
    public StrimziAclOperation getOperation() {
        return operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleAclRule that = (SimpleAclRule) o;

        if (type != that.type) return false;
        if (!resource.equals(that.resource)) return false;
        if (!host.equals(that.host)) return false;
        return operation == that.operation;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + resource.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + operation.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SimpleAclRule(" +
                "type: " + type + ", " +
                "resource: " + resource + ", " +
                "host: " + host + ", " +
                "operation: " + operation + ")";
    }

    /**
     * Create Kafka's AclBinding instance from current SimpleAclRule instance for the provided principal
     *
     * @param principal KafkaPrincipal instance for the current SimpleAclRule
     * @return Kafka AclBinding instance
     */
    public AclBinding toKafkaAclBinding(KafkaPrincipal principal) {
        ResourcePattern resourcePattern = resource.toKafkaResourcePattern();
        AclPermissionType kafkaType = toKafkaAclPermissionType(type);
        AclOperation kafkaOperation = toKafkaAclOperation(operation);
        return new AclBinding(resourcePattern, new AccessControlEntry(principal.toString(), getHost(), kafkaOperation, kafkaType));
    }

    /**
     * Creates SimpleAclRule instance based on Kafka's AclBinding instance containing the resource the rule should apply to.
     *
     * @param aclBinding the AclBinding instance which should be used to create the rule
     * @return the SimpleAclRule instance
     */
    public static SimpleAclRule fromAclBinding(AclBinding aclBinding) {
        SimpleAclRuleResource resource = SimpleAclRuleResource.fromKafkaResourcePattern(aclBinding.pattern());
        AclRuleType type = fromKafkaAclPermissionType(aclBinding.entry().permissionType());
        StrimziAclOperation operation = fromKafkaAclOperation(aclBinding.entry().operation());
        return new SimpleAclRule(type, resource, aclBinding.entry().host(), operation);
    }

    /**
     * Creates SimpleAclRule object based on AclRule object which is received as part of the KafkaUser CRD.
     *
     * @param rule AclRule object from KafkaUser CR
     * @return The SimpleAclRule.
     */
    public static List<SimpleAclRule> fromCrd(AclRule rule) {
        if (rule.getOperations() != null) {
            List<SimpleAclRule> simpleAclRules = new ArrayList<>();
            for (StrimziAclOperation operation : rule.getOperations()) {
                simpleAclRules.add(new SimpleAclRule(rule.getType(), SimpleAclRuleResource.fromCrd(rule.getResource()), rule.getHost(), operation));
            }
            return simpleAclRules;
        } else {
            // Should not happen, because in v1 CRD the operations field is required.
            // But you never know, so we better handle it to be sure.
            return List.of();
        }
    }

    private AclPermissionType toKafkaAclPermissionType(AclRuleType aclRuleType) {
        return switch (aclRuleType) {
            case DENY -> AclPermissionType.DENY;
            case ALLOW -> AclPermissionType.ALLOW;
        };
    }

    private AclOperation toKafkaAclOperation(StrimziAclOperation operation) {
        return switch (operation) {
            case READ -> AclOperation.READ;
            case WRITE -> AclOperation.WRITE;
            case CREATE -> AclOperation.CREATE;
            case DELETE -> AclOperation.DELETE;
            case ALTER -> AclOperation.ALTER;
            case DESCRIBE -> AclOperation.DESCRIBE;
            case CLUSTERACTION -> AclOperation.CLUSTER_ACTION;
            case ALTERCONFIGS -> AclOperation.ALTER_CONFIGS;
            case DESCRIBECONFIGS -> AclOperation.DESCRIBE_CONFIGS;
            case IDEMPOTENTWRITE -> AclOperation.IDEMPOTENT_WRITE;
            case ALL -> AclOperation.ALL;
        };
    }

    private static AclRuleType fromKafkaAclPermissionType(AclPermissionType aclPermissionType) {
        return switch (aclPermissionType) {
            case DENY -> AclRuleType.DENY;
            case ALLOW -> AclRuleType.ALLOW;
            default -> throw new IllegalArgumentException("Invalid AclRule type: " + aclPermissionType);
        };
    }

    private static StrimziAclOperation fromKafkaAclOperation(AclOperation aclOperation) {
        return switch (aclOperation) {
            case READ -> StrimziAclOperation.READ;
            case WRITE -> StrimziAclOperation.WRITE;
            case CREATE -> StrimziAclOperation.CREATE;
            case DELETE -> StrimziAclOperation.DELETE;
            case ALTER -> StrimziAclOperation.ALTER;
            case DESCRIBE -> StrimziAclOperation.DESCRIBE;
            case CLUSTER_ACTION -> StrimziAclOperation.CLUSTERACTION;
            case ALTER_CONFIGS -> StrimziAclOperation.ALTERCONFIGS;
            case DESCRIBE_CONFIGS -> StrimziAclOperation.DESCRIBECONFIGS;
            case IDEMPOTENT_WRITE -> StrimziAclOperation.IDEMPOTENTWRITE;
            case ALL -> StrimziAclOperation.ALL;
            default -> throw new IllegalArgumentException("Invalid AclRule operation: " + aclOperation);
        };
    }
}
