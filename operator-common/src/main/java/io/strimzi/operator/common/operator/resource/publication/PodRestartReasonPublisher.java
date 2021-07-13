/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.operator.resource.publication;

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
public class PodRestartReasonPublisher {

    private final KubernetesRestartEventPublisher k8sPublisher;
    private final MicrometerRestartEventPublisher micrometerPublisher;
    private final Map<RestartReason, Counter> counters = new HashMap<>();

    public PodRestartReasonPublisher(KubernetesClient kubernetesClient, MetricsProvider metricsProvider, String operatorId) {
        micrometerPublisher = new MicrometerRestartEventPublisher(metricsProvider);
        k8sPublisher = new KubernetesRestartEventPublisher(kubernetesClient, operatorId);
    }

    public void publish(Pod restartingPod, RestartReasons reasons) {
        k8sPublisher.publishRestartEvent(restartingPod, reasons);
        micrometerPublisher.publishRestartEvent(restartingPod, reasons);
    }
}
