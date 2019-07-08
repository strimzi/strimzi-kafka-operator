/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.operator.resource;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.vertx.core.AsyncResult;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ConditionUtils {

    public static Condition buildConditionFromReconciliationResult(AsyncResult<Void> reconciliationResult) {
        Condition readyCondition;
        if (reconciliationResult.succeeded()) {
            readyCondition = new ConditionBuilder()
                    .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                    .withNewType("Ready")
                    .withNewStatus("True")
                    .build();
        } else {
            readyCondition = new ConditionBuilder()
                    .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                    .withNewType("NotReady")
                    .withNewStatus("True")
                    .withNewReason(reconciliationResult.cause().getClass().getSimpleName())
                    .withNewMessage(reconciliationResult.cause().getMessage())
                    .build();
        }
        return readyCondition;
    }

    private static Date dateSupplier() {
        return new Date();
    }
}
