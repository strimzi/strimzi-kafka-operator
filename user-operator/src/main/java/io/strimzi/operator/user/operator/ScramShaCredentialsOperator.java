/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.List;

public class ScramShaCredentialsOperator {

    private ScramShaCredentials credsManager;

    private Vertx vertx;

    public ScramShaCredentialsOperator(Vertx vertx, ScramShaCredentials credsManager) {
        this.credsManager = credsManager;
        this.vertx = vertx;
    }

    Future<Void> reconcile(Reconciliation reconciliation, String username, String password) {
        Promise<Void> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                boolean exists = credsManager.exists(reconciliation, username);
                if (password != null) {
                    credsManager.createOrUpdate(reconciliation, username, password);
                    future.complete(null);
                } else  {
                    if (exists) {
                        credsManager.delete(reconciliation, username);
                        future.complete(null);
                    } else {
                        future.complete(null);
                    }
                }
            },
            false,
            promise);
        return promise.future();
    }

    public List<String> list(Reconciliation reconciliation) {
        return credsManager.list(reconciliation);
    }
}
