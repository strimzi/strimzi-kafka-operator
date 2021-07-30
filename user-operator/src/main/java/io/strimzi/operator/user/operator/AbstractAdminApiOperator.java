/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;

import java.util.Collection;

/**
 * Abstract operator using the Kafka Admin API
 */
public abstract class AbstractAdminApiOperator<T, S extends Collection<String>> {
    protected final Vertx vertx;
    protected final Admin adminClient;

    /**
     * Constructor
     *
     * @param vertx Vertx instance
     * @param adminClient Kafka Admin client instance
     */
    public AbstractAdminApiOperator(Vertx vertx, Admin adminClient)  {
        this.vertx = vertx;
        this.adminClient = adminClient;
    }

    /**
     * Reconcile using Kafka Admin API
     *
     * @param reconciliation The reconciliation
     * @param username  User name of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The desired object
     * @return the Future with reconcile result
     */
    public abstract Future<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String username, T desired);

    /**
     * Returns set with all usernames which have some value set right now
     *
     * @return The set with all usernames which have some value set right now
     */
    public abstract Future<S> getAllUsers();
}
