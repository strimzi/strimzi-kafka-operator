/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.publication;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.RestartReasons;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.KubernetesEventsPublisher;
import io.strimzi.operator.common.operator.resource.publication.micrometer.MicrometerRestartEventsPublisher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Creates Kubernetes events, and increments metric counters, to make Strimzi initiated restarts, and reasons why
 * more observable for people responsible for running Strimzi
 */
public class PodRestartReasonPublisher {

    private static final Logger LOG = LogManager.getLogger(PodRestartReasonPublisher.class);

    private final RestartEventsPublisher k8sPublisher;
    private final RestartEventsPublisher micrometerPublisher;

    public PodRestartReasonPublisher(KubernetesClient kubernetesClient, MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, String operatorId) {
        micrometerPublisher = new MicrometerRestartEventsPublisher(metricsProvider);
        k8sPublisher = KubernetesEventsPublisher.createPublisher(kubernetesClient, operatorId, pfa.getHighestEventApiVersion());
    }

    /**
     * Emit events to Kubernetes events api, and to the Micrometer registry for scraping
     * @param restartingPod pod being restarted
     * @param reasons collection of 1 to N reasons why the pod needs to be restarted
     */
    public void publish(Pod restartingPod, RestartReasons reasons) {
        LOG.debug("Publishing restart for pod {} for {}", restartingPod.getMetadata().getName(), reasons.getAllReasonNotes());
        k8sPublisher.publishRestartEvents(restartingPod, reasons);
        micrometerPublisher.publishRestartEvents(restartingPod, reasons);
    }
}
