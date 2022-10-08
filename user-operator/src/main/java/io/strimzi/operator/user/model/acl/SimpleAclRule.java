/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.AclRuleType;

import io.strimzi.operator.cluster.model.InvalidResourceException;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.List;
import java.util.ArrayList;

/**
 * Immutable class which represents a single ACL rule for AclAuthorizer.
 * The main reason for not using directly the classes from the api module is that we need immutable objects for use in Sets.
 */
public class SimpleAclRule {
    private final AclRuleType type;
    private final SimpleAclRuleResource resource;
    private final String host;
    private final AclOperation operation;

    /**
     * Constructor
     *
     * @param type      Type of the Acl rule (Allow or Deny)
     * @param resource  The resource to which this rule applies (Topic, Group, Cluster, ...)
     * @param host      The host from which is this rule allowed / denied
     * @param operation The Operation which is allowed or denied
     */
    public SimpleAclRule(AclRuleType type, SimpleAclRuleResource resource, String host, AclOperation operation) {
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
    public AclOperation getOperation() {
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
        org.apache.kafka.common.acl.AclOperation kafkaOperation = toKafkaAclOperation(operation);
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
        AclOperation operation = fromKafkaAclOperation(aclBinding.entry().operation());
        return new SimpleAclRule(type, resource, aclBinding.entry().host(), operation);
    }

    /**
     * Creates SimpleAclRule object based on AclRule object which is received as part of the KafkaUser CRD.
     *
     * @param rule AclRule object from KafkaUser CR
     * @return The SimpleAclRule.
     */
    @SuppressWarnings("deprecation")
    public static List<SimpleAclRule> fromCrd(AclRule rule) {
        if (rule.getOperations() != null && rule.getOperation() != null) {
            throw new InvalidResourceException("Both fields `operations` and `operation` cannot be filled in at the same time");
        } else if (rule.getOperations() != null) {
            List<SimpleAclRule> simpleAclRules = new ArrayList<>();
            for (AclOperation operation : rule.getOperations()) {
                simpleAclRules.add(new SimpleAclRule(rule.getType(), SimpleAclRuleResource.fromCrd(rule.getResource()), rule.getHost(), operation));
            }
            return simpleAclRules;
        } else {
            return List.of(new SimpleAclRule(rule.getType(), SimpleAclRuleResource.fromCrd(rule.getResource()), rule.getHost(), rule.getOperation()));
        }
    }

    private AclPermissionType toKafkaAclPermissionType(AclRuleType aclRuleType) {
        switch (aclRuleType) {
            case DENY:
                return AclPermissionType.DENY;
            case ALLOW:
                return AclPermissionType.ALLOW;
            default:
                throw new IllegalArgumentException("Invalid Acl type: " + aclRuleType);
        }
    }

    private org.apache.kafka.common.acl.AclOperation toKafkaAclOperation(AclOperation operation) {
        switch (operation) {
            case READ:
                return org.apache.kafka.common.acl.AclOperation.READ;
            case WRITE:
                return org.apache.kafka.common.acl.AclOperation.WRITE;
            case CREATE:
                return org.apache.kafka.common.acl.AclOperation.CREATE;
            case DELETE:
                return org.apache.kafka.common.acl.AclOperation.DELETE;
            case ALTER:
                return org.apache.kafka.common.acl.AclOperation.ALTER;
            case DESCRIBE:
                return org.apache.kafka.common.acl.AclOperation.DESCRIBE;
            case CLUSTERACTION:
                return org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION;
            case ALTERCONFIGS:
                return org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
            case DESCRIBECONFIGS:
                return org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
            case IDEMPOTENTWRITE:
                return org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
            case ALL:
                return org.apache.kafka.common.acl.AclOperation.ALL;
            default:
                throw new IllegalArgumentException("Invalid Acl operation: " + operation);
        }
    }

    private static AclRuleType fromKafkaAclPermissionType(AclPermissionType aclPermissionType) {
        switch (aclPermissionType) {
            case DENY:
                return AclRuleType.DENY;
            case ALLOW:
                return AclRuleType.ALLOW;
            default:
                throw new IllegalArgumentException("Invalid AclRule type: " + aclPermissionType);
        }
    }

    private static AclOperation fromKafkaAclOperation(org.apache.kafka.common.acl.AclOperation aclOperation) {
        switch (aclOperation) {
            case READ:
                return AclOperation.READ;
            case WRITE:
                return AclOperation.WRITE;
            case CREATE:
                return AclOperation.CREATE;
            case DELETE:
                return AclOperation.DELETE;
            case ALTER:
                return AclOperation.ALTER;
            case DESCRIBE:
                return AclOperation.DESCRIBE;
            case CLUSTER_ACTION:
                return AclOperation.CLUSTERACTION;
            case ALTER_CONFIGS:
                return AclOperation.ALTERCONFIGS;
            case DESCRIBE_CONFIGS:
                return AclOperation.DESCRIBECONFIGS;
            case IDEMPOTENT_WRITE:
                return AclOperation.IDEMPOTENTWRITE;
            case ALL:
                return AclOperation.ALL;
            default:
                throw new IllegalArgumentException("Invalid AclRule operation: " + aclOperation);
        }
    }
}
