/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.AclRuleType;

import kafka.security.auth.Acl;
import kafka.security.auth.All$;
import kafka.security.auth.Allow$;
import kafka.security.auth.Alter$;
import kafka.security.auth.AlterConfigs$;
import kafka.security.auth.ClusterAction$;
import kafka.security.auth.Create$;
import kafka.security.auth.Delete$;
import kafka.security.auth.Deny$;
import kafka.security.auth.Describe$;
import kafka.security.auth.DescribeConfigs$;
import kafka.security.auth.IdempotentWrite$;
import kafka.security.auth.Operation;
import kafka.security.auth.PermissionType;
import kafka.security.auth.Read$;
import kafka.security.auth.Write$;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Immutable class which represents a single ACL rule for SimpleAclAuthorizer.
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
     * @param type          Type of the Acl rule (Allow or Deny)
     * @param resource      The resource to which this rule applies (Topic, Group, Cluster, ...)
     * @param host          The host from which is this rule allowed / denied
     * @param operation     The Operation which is allowed or denied
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
     * Create Kafka's Acl object from SimpleAclRule object.
     *
     * @param principal Kafka prinmcipal needed to create Kafka's Acl object.
     * @return The Kafka ACL.
     */
    public Acl toKafkaAcl(KafkaPrincipal principal)   {
        PermissionType kafkaType;
        Operation kafkaOperation;

        switch (type) {
            case DENY:
                kafkaType = Deny$.MODULE$;
                break;
            case ALLOW:
                kafkaType = Allow$.MODULE$;
                break;
            default:
                throw new IllegalArgumentException("Invalid Acl type: " + type);
        }

        switch (operation) {
            case READ:
                kafkaOperation = Read$.MODULE$;
                break;
            case WRITE:
                kafkaOperation = Write$.MODULE$;
                break;
            case CREATE:
                kafkaOperation = Create$.MODULE$;
                break;
            case DELETE:
                kafkaOperation = Delete$.MODULE$;
                break;
            case ALTER:
                kafkaOperation = Alter$.MODULE$;
                break;
            case DESCRIBE:
                kafkaOperation = Describe$.MODULE$;
                break;
            case CLUSTERACTION:
                kafkaOperation = ClusterAction$.MODULE$;
                break;
            case ALTERCONFIGS:
                kafkaOperation = AlterConfigs$.MODULE$;
                break;
            case DESCRIBECONFIGS:
                kafkaOperation = DescribeConfigs$.MODULE$;
                break;
            case IDEMPOTENTWRITE:
                kafkaOperation = IdempotentWrite$.MODULE$;
                break;
            case ALL:
                kafkaOperation = All$.MODULE$;
                break;
            default:
                throw new IllegalArgumentException("Invalid Acl operation: " + operation);
        }

        return new Acl(principal, kafkaType, getHost(), kafkaOperation);
    }

    /**
     * Creates SimpleAclRule object based on Kafka's Acl object and an resource the rule should apply to.
     *
     * @param resource  The resource the newly created rule should apply to
     * @param acl       The Acl object which should be used to create the rule
     * @return The SimpleAclRule.
     */
    public static SimpleAclRule fromKafkaAcl(SimpleAclRuleResource resource, Acl acl)   {
        AclRuleType type;
        AclOperation operation;

        switch (acl.permissionType().toJava()) {
            case DENY:
                type = AclRuleType.DENY;
                break;
            case ALLOW:
                type = AclRuleType.ALLOW;
                break;
            default:
                throw new IllegalArgumentException("Invalid AclRule type: " + acl.permissionType().toJava());
        }

        switch (acl.operation().toJava()) {
            case READ:
                operation = AclOperation.READ;
                break;
            case WRITE:
                operation = AclOperation.WRITE;
                break;
            case CREATE:
                operation = AclOperation.CREATE;
                break;
            case DELETE:
                operation = AclOperation.DELETE;
                break;
            case ALTER:
                operation = AclOperation.ALTER;
                break;
            case DESCRIBE:
                operation = AclOperation.DESCRIBE;
                break;
            case CLUSTER_ACTION:
                operation = AclOperation.CLUSTERACTION;
                break;
            case ALTER_CONFIGS:
                operation = AclOperation.ALTERCONFIGS;
                break;
            case DESCRIBE_CONFIGS:
                operation = AclOperation.DESCRIBECONFIGS;
                break;
            case IDEMPOTENT_WRITE:
                operation = AclOperation.IDEMPOTENTWRITE;
                break;
            case ALL:
                operation = AclOperation.ALL;
                break;
            default:
                throw new IllegalArgumentException("Invalid AclRule operation: " + acl.operation().toJava());
        }

        return new SimpleAclRule(type, resource, acl.host(), operation);
    }

    /**
     * Creates SimpleAclRule object based on AclRule object which is received as part ofthe KafkaUser CRD.
     *
     * @param rule  AclRule object from KafkaUser CR
     * @return The SimpleAclRule.
     */
    public static SimpleAclRule fromCrd(AclRule rule)   {
        return new SimpleAclRule(rule.getType(), SimpleAclRuleResource.fromCrd(rule.getResource()), rule.getHost(), rule.getOperation());
    }
}
