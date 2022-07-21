/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.events.v1.EventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.time.Clock;

/**
 * Publishes K8s events in the events.k8s.io/v1 format
 */
class V1RestartEventPublisher extends KubernetesRestartEventPublisher {

    private final KubernetesClient client;
    private final String operatorName;

    V1RestartEventPublisher(Clock clock, KubernetesClient client, String operatorName) {
        super(clock);
        this.client = client;
        this.operatorName = operatorName;
    }

    @Override
    protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
        EventBuilder builder = new EventBuilder();

        builder.withNewMetadata()
                    .withGenerateName("strimzi-event")
                .endMetadata()
                .withAction(ACTION)
                .withReportingController(CONTROLLER)
                .withReportingInstance(operatorName)
                .withRegarding(podReference)
                .withReason(reason)
                .withType(type)
                .withEventTime(eventTime)
                .withNote(note);

        client.events().v1().events().inNamespace(podReference.getNamespace()).resource(builder.build()).create();
    }
}

