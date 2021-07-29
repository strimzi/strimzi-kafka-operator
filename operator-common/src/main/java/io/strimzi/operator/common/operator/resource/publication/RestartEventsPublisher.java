/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.publication;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.common.model.RestartReasons;

public interface RestartEventsPublisher {

    //TODO add Reconcilation metadata like Stanislav's approach
    void publishRestartEvents(Pod pod, RestartReasons reasons);
}
