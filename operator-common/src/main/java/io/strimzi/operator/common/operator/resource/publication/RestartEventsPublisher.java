package io.strimzi.operator.common.operator.resource.publication;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.common.model.RestartReasons;

import java.time.ZonedDateTime;

public interface RestartEventsPublisher {

    //TODO add Reconcilation metadata like Stanislav's approach
    void publishRestartEvents(Pod pod, RestartReasons reasons);
}
