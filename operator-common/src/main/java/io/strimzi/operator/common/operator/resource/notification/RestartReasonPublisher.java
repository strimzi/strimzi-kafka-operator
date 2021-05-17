/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.operator.resource.notification;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Counter;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.RestartReason;
import io.strimzi.operator.common.model.RestartReasons;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class RestartReasonPublisher {

    private final KubernetesClient kubernetesClient;
    private final MetricsProvider metricsProvider;
    private final Map<RestartReason, Counter> counters = new HashMap<>();

    public RestartReasonPublisher(KubernetesClient kubernetesClient, MetricsProvider metricsProvider) {
        this.kubernetesClient = kubernetesClient;
        this.metricsProvider = metricsProvider;
    }

    public void publish(Pod restartingPod, RestartReasons reasons) {

    }

    private void publishKubernetesEvent() {

    }

    private void publishCounter(RestartReason reason) {

    }

}
