/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.events.v1.EventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Publishes KafkaRoller restart events as Kubernetes events
 */
public class KubernetesRestartEventPublisher {

    private static final Logger LOG = LogManager.getLogger(KubernetesRestartEventPublisher.class);
    private static final DateTimeFormatter K8S_MICROTIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSSSSSXXX");

    protected static final String ACTION = "StrimziInitiatedPodRestart";
    protected static final String CONTROLLER = "strimzi.io/cluster-operator";

    // K8s events are required to have a message of 1KiB or smaller
    private static final int MAX_MESSAGE_LENGTH = 1000;
    private static final String ELLIPSIS = "...";

    private final Clock clock;
    private final String operatorName;
    private final KubernetesClient client;

    /**
     * Create an instance for the event API.
     *
     * @param client       Kubernetes client
     * @param operatorName the pod name of the current cluster operator instance
     */
    public KubernetesRestartEventPublisher(KubernetesClient client, String operatorName) {
        this(client, operatorName, Clock.systemDefaultZone());
    }

    protected KubernetesRestartEventPublisher(KubernetesClient client, String operatorName, Clock clock) {
        this.clock = clock;
        this.operatorName = operatorName;
        this.client = client;
    }

    /**
     * Publishes an Kubernetes Event about Pod restart
     *
     * @param pod       Pod which is restarted
     * @param reasons   Reasons for the restart
     */
    public void publishRestartEvents(Pod pod, RestartReasons reasons) {
        MicroTime k8sEventTime = new MicroTime(K8S_MICROTIME.format(ZonedDateTime.now(clock)));
        ObjectReference podReference = createPodReference(pod);

        try {
            for (RestartReason reason : reasons) {
                String note = maybeTruncated(reasons.getNoteFor(reason));
                String type = "Normal";
                String k8sFormattedReason = reason.pascalCased();
                LOG.debug("Publishing K8s event, time {}, type, {}, reason, {}, note, {}, pod, {}",
                        k8sEventTime, type, k8sFormattedReason, note, podReference);
                publishEvent(k8sEventTime, podReference, k8sFormattedReason, type, note);
            }
        } catch (Exception e) {
            LOG.error("Exception on K8s event publication", e);
        }
    }

    /**
     * Publish a Kubernetes Event referring to certain KafkaRoller pod action
     *
     * @param eventTime    - Microtime to use for event
     * @param podReference - ObjectReference pointing to rolled pod
     * @param reason       - reason the pod is being rolled
     * @param type         - the type of K8s event "Normal", or "Warning"
     * @param note         - the note to attach to the event
     */
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

    ObjectReference createPodReference(Pod pod) {
        return new ObjectReferenceBuilder().withKind("Pod")
                                           .withNamespace(pod.getMetadata().getNamespace())
                                           .withName(pod.getMetadata().getName())
                                           .build();
    }


    /**
     * While the core event API doesn't set a limit on note sizes, events.k8s.io/v1beta1 and v1 do, which is 1kB.
     * It's a reasonably safe bet that notes will only include characters in the ASCII subset, so that's all that is supported.
     * An exception is thrown if multibyte characters appear in the note.
     *
     * @param note the candidate string for truncation
     * @return the note unchanged if <=1kB, truncated otherwise, with the last 3 characters being "..." to indicate truncation
     * @throws UnsupportedOperationException if the note contains multibyte characters when represented as UTF-8
     */
    String maybeTruncated(String note) throws UnsupportedOperationException {
        byte[] stringBytes = note.getBytes(UTF_8);

        if (note.length() != stringBytes.length) {
            throw new UnsupportedOperationException("Truncating messages containing multibyte characters isn't implemented");
        } else if (stringBytes.length <= MAX_MESSAGE_LENGTH) {
            return note;
        } else {
            return new String(stringBytes, 0, 997, UTF_8) + ELLIPSIS;
        }
    }
}
