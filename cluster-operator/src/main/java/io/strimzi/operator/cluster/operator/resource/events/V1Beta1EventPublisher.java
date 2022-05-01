/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.events.v1beta1.EventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;

import java.time.Clock;

/**
 * Publishes K8s events in the events.k8s.io/v1beta1 format
 */
class V1Beta1EventPublisher extends KubernetesRestartEventPublisher {

    private final KubernetesClient client;
    private final String operatorName;

    V1Beta1EventPublisher(Clock clock, KubernetesClient client, String operatorName) {
        super(clock);
        this.client = client;
        this.operatorName = operatorName;
    }


    @Override
    protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
        EventBuilder builder = new EventBuilder();

        builder.withAction(action)
                .withNewMetadata()
                    .withGenerateName("strimzi-event")
                .endMetadata()
                .withReportingController(controller)
                .withReportingInstance(operatorName)
                .withRegarding(podReference)
                .withReason(reason)
                .withType(type)
                .withEventTime(eventTime)
                .withNote(note);

        client.events().v1beta1().events().create(builder.build());
    }
}
