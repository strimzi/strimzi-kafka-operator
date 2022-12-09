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
 * Interface for operators using the Kafka Admin API
 */
public interface AdminApiOperator<T, S extends Collection<String>> {
    /**
     * Reconcile using Kafka Admin API
     *
     * @param reconciliation The reconciliation
     * @param username  Username of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The desired object
     * @return the Future with reconcile result
     */
    CompletionStage<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String username, T desired);

    /**
     * Returns set with all usernames which have some value set right now
     *
     * @return The set with all usernames which have some value set right now
     */
    CompletionStage<S> getAllUsers();

    /**
     * Starts the API Operator - this is used for example to start the Cache and BatchReconcilers
     */
    void start();

    /**
     * Stops the Admin API Operator - this is used for example to stop the Cache and BatchReconcilers
     */
    void stop();

    /**
     * Class used to pass the reconciliation results
     *
     * @param <T>            Request type (type of the desired resource)
     * @param <R>            Response type (type of the repsonse)
     * @param reconciliation Reconciliation marker
     * @param username       Name of the user
     * @param desired        Desired resource
     * @param result         Completable future for the reconciliation result
     */
    record ReconcileRequest<T, R>(Reconciliation reconciliation, String username, T desired, CompletableFuture<R> result) { }
}
