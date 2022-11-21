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

    /**
     * Class used to pass the reconciliation results
     *
     * @param <T>   Request type (type of the desired resource)
     * @param <R>   Response type (type of the repsonse)
     */
    public static class ReconcileRequest<T, R>    {
        /**
         * Reconciliation marker
         */
        public final Reconciliation reconciliation;

        /**
         * Name of the user
         */
        public final String username;

        /**
         * Desired resource
         */
        public final T desired;

        /**
         * Completable future for the reconciliation result
         */
        public final CompletableFuture<R> result;

        /**
         * Constructs the reconciliation result
         *
         * @param reconciliation    Reconciliation marker
         * @param username          Name of the user
         * @param desired           Desired resource
         * @param result            Completable future for the reconciliation result
         */
        public ReconcileRequest(Reconciliation reconciliation, String username, T desired, CompletableFuture<R> result) {
            this.reconciliation = reconciliation;
            this.username = username;
            this.desired = desired;
            this.result = result;
        }
    }
}
