/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;

public class ScramShaCredentialsOperator {

    private ScramShaCredentials credsManager;

    private Vertx vertx;

    public ScramShaCredentialsOperator(Vertx vertx, ScramShaCredentials credsManager) {
        this.credsManager = credsManager;
        this.vertx = vertx;
    }

    Future<ReconcileResult<Void>> reconcile(String username, String password) {
        Future<ReconcileResult<Void>> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                boolean exists = credsManager.exists(username);
                if (password != null) {
                    credsManager.createOrUpdate(username, password, -1);
                    future.complete(exists ? ReconcileResult.created(null) : ReconcileResult.patched(null));
                } else  {
                    if (exists) {
                        credsManager.delete(username);
                        future.complete(ReconcileResult.deleted());
                    } else {
                        future.complete(ReconcileResult.noop());
                    }
                }
            },
            false,
            fut.completer());
        return fut;
    }

    public List<String> list() {
        return credsManager.list();
    }
}
