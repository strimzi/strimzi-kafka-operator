package io.strimzi.operator.common.operator.resource.publication.kubernetes.versions;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.events.v1beta1.EventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.KubernetesEventsPublisher;

import java.time.Clock;


public class V1Beta1EventPublisher extends KubernetesEventsPublisher {

    private final KubernetesClient client;
    private final String operatorId;

    public V1Beta1EventPublisher(Clock clock, KubernetesClient client, String operatorId) {
        super(clock);
        this.client = client;
        this.operatorId = operatorId;
    }


    @Override
    protected void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note) {
        EventBuilder builder = new EventBuilder();

        builder.withAction(action)
                .withNewMetadata()
                    .withGenerateName("strimzi-event")
                .endMetadata()
                .withReportingController(controller)
                .withReportingInstance(operatorId)
                .withRegarding(podReference)
                .withReason(reason)
                .withType(type)
                .withEventTime(eventTime)
                .withNote(note);

        client.events().v1beta1().events().create(builder.build());
    }
}
