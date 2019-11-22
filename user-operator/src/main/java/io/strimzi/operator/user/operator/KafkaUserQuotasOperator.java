/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class KafkaUserQuotasOperator {

    private KafkaUserQuotas quotasManager;

    private Vertx vertx;

    public KafkaUserQuotasOperator(Vertx vertx, KafkaUserQuotas quotasManager) {
        this.quotasManager = quotasManager;
        this.vertx = vertx;
    }

    Future<ReconcileResult<Void>> reconcile(String username, JsonObject quotas) {
        Future<ReconcileResult<Void>> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    boolean exists = quotasManager.exists(username);
                    if (quotas != null) {
                        quotasManager.createOrUpdate(username, quotas);
                        future.complete(exists ? ReconcileResult.created(null) : ReconcileResult.patched(null));
                    } else {
                        if (exists) {
                            quotasManager.delete(username);
                            future.complete(ReconcileResult.deleted());
                        } else {
                            future.complete(ReconcileResult.noop(null));
                        }
                    }
                } catch (Throwable t) {
                    fut.fail(t);
                }
            },
            false,
            fut);
        return fut;
    }
}
