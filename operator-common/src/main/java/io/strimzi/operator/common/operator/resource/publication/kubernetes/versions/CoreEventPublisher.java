package io.strimzi.operator.common.operator.resource.publication.kubernetes.versions;

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.KubernetesEventsPublisher;

import java.time.Clock;


public class CoreEventPublisher extends KubernetesEventsPublisher {

    private final KubernetesClient client;
    private final String operatorId;

    public CoreEventPublisher(Clock clock, KubernetesClient client, String operatorId) {
        super(clock);
        this.client = client;
        this.operatorId = operatorId;
    }

    @Override
    protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String message) {
        EventBuilder builder = new EventBuilder();

        builder.withAction(action)
                .withReportingComponent(controller)
                .withReportingInstance(operatorId)
                .withInvolvedObject(podReference)
                .withReason(reason)
                .withType(type)
                .withEventTime(eventTime)
                .withMessage(message);

        client.v1().events().create(builder.build());
    }
}
