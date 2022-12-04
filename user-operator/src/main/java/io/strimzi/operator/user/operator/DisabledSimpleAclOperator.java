/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.acl.SimpleAclRule;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * DisabledSimpleAclOperator is used when the management of ACL rules is not allowed. It does not provide any
 * functionality and throws UnsupportedOperationException exception when used. The logic in KafkaUserOperator class
 * should make sure it is never called, but having this special implementation makes sure we do not need to keep around
 * the proper SimpleAclOperator with its cache and batch reconciler etc. And in case it is somewhere called by mistake,
 * the unsupported exceptions make it easier to detect it.
 */
public class DisabledSimpleAclOperator implements AdminApiOperator<Set<SimpleAclRule>, Set<String>> {
    /**
     * Constructor
     */
    public DisabledSimpleAclOperator() {
        // Nothing to do here => the DisabledSimpleAclOperator just throws unsupported exceptions
    }

    @Override
    public CompletionStage<ReconcileResult<Set<SimpleAclRule>>> reconcile(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("DisabledSimpleAclOperator cannot be used to reconcile users"));
    }

    @Override
    public CompletionStage<Set<String>> getAllUsers() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("DisabledSimpleAclOperator cannot be used to get list of all users"));
    }

    @Override
    public void start() {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to do
    }
}
