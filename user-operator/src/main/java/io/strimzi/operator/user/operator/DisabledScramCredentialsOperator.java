/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * DisabledScramCredentialsOperator is used when the use of SCRAM-SHA users is not allowed. It does not provide any
 * functionality and throws UnsupportedOperationException exception when used. The logic in KafkaUserOperator class
 * should make sure it is never called, but having this special implementation makes sure we do not need to keep around
 * the proper ScramCredentialsOperator with its cache and batch reconciler etc. And in case it is somewhere called by
 * mistake, the unsupported exceptions make it easier to detect it.
 */
public class DisabledScramCredentialsOperator implements AdminApiOperator<String, List<String>> {
    /**
     * Constructor
     */
    public DisabledScramCredentialsOperator() {
        // Nothing to do here => the DisabledScramCredentialsOperator just throws unsupported exceptions
    }

    @Override
    public CompletionStage<ReconcileResult<String>> reconcile(Reconciliation reconciliation, String username, String desired) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("DisabledScramCredentialsOperator cannot be used to reconcile users"));
    }

    @Override
    public CompletionStage<List<String>> getAllUsers() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("DisabledScramCredentialsOperator cannot be used to get list of all users"));
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
