package io.strimzi.operator.common.operator.resource.publication;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.RestartReasons;

public class MicrometerRestartEventPublisher{

    public MicrometerRestartEventPublisher(MetricsProvider metricsProvider) {

    }

    public void publishRestartEvent(Pod pod, RestartReasons reasons) {

    }
}
