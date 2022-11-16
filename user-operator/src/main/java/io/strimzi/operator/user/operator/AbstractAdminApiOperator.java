/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Abstract operator using the Kafka Admin API
 */
public abstract class AbstractAdminApiOperator<T, S extends Collection<String>> {
    /**
     * Constructor
     */
    public AbstractAdminApiOperator()  {
        // Nothing to do
    }

    /**
     * Reconcile using Kafka Admin API
     *
     * @param reconciliation The reconciliation
     * @param username  User name of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The desired object
     * @return the Future with reconcile result
     */
    public abstract CompletionStage<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String username, T desired);

    /**
     * Returns set with all usernames which have some value set right now
     *
     * @return The set with all usernames which have some value set right now
     */
    public abstract CompletionStage<S> getAllUsers();

    public static class ReconcileRequest<T, R>    {
        public final Reconciliation reconciliation;
        public final String username;
        public final T desired;
        public final CompletableFuture<R> result;

        public ReconcileRequest(Reconciliation reconciliation, String username, T desired, CompletableFuture<R> result) {
            this.reconciliation = reconciliation;
            this.username = username;
            this.desired = desired;
            this.result = result;
        }
    }
}
