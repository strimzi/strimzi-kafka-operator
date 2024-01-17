/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.StatusUtils;

import java.util.ArrayList;
import java.util.Set;

/**
 * Various utility methods used by the UserOperatorController to make leaner and easier to read
 */
public class UserControllerUtils {
    /**
     * Creates a new KafkaUSer status which indicates that the resource reconciliation is paused
     *
     * @param reconciliation    Reconciliation marker
     * @param user              The KafkaUser resource for which the status should be created
     *
     * @return  A new paused status for the KafkaUser resource
     */
    public static KafkaUserStatus pausedStatus(Reconciliation reconciliation, KafkaUser user)  {
        KafkaUserStatus status = new KafkaUserStatus();

        Set<Condition> conditions = StatusUtils.validate(reconciliation, user);
        conditions.add(StatusUtils.getPausedCondition());
        status.setConditions(new ArrayList<>(conditions));
        status.setObservedGeneration(user.getStatus() != null ? user.getStatus().getObservedGeneration() : 0);

        return status;
    }
}
