/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

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

    Future<Void> reconcile(String username, String password) {
        Promise<Void> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                boolean exists = credsManager.exists(username);
                if (password != null) {
                    credsManager.createOrUpdate(username, password);
                    future.complete(null);
                } else  {
                    if (exists) {
                        credsManager.delete(username);
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

    public List<String> list() {
        return credsManager.list();
    }
}
