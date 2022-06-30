/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
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
 * Publishes KafkaRoller restart events as Kubernetes events, using either the events.k8s.io/v1beta1 or v1 API,
 * depending on which is available.
 */
public abstract class KubernetesRestartEventPublisher {

    private static final Logger LOG = LogManager.getLogger(KubernetesRestartEventPublisher.class);
    private final Clock clock;
    private static final DateTimeFormatter K8S_MICROTIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSSSSSXXX");

    protected static final String ACTION = "StrimziInitiatedPodRestart";
    protected static final String CONTROLLER = "strimzi.io/cluster-operator";

    // K8s events are required to have a message of 1KiB or smaller
    private static final int MAX_MESSAGE_LENGTH = 1000;
    private static final String ELLIPSIS = "...";

    public KubernetesRestartEventPublisher() {
        this(Clock.systemDefaultZone());
    }

    protected KubernetesRestartEventPublisher(Clock clock) {
        this.clock = clock;
    }

    /**
     * Create an instance for the highest event API, there's three possible subtypes, due to how fabric8 delineates the differing
     * event APIs.
     *
     * @param client       Kubernetes client
     * @param operatorName the pod name of the current cluster operator instance
     * @param hasEventsV1  if the cluster is using events.k8s.io/v1 instead of events.k8s.io/v1beta1
     * @return instance of the appropriate publisher for the given API
     */
    public static KubernetesRestartEventPublisher createPublisher(KubernetesClient client, String operatorName, boolean hasEventsV1) {
        Clock clock = Clock.systemDefaultZone();
        if (hasEventsV1) {
            return new V1RestartEventPublisher(clock, client, operatorName);
        } else {
            return new V1Beta1RestartEventPublisher(clock, client, operatorName);
        }
    }

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
     * Implemented by subclasses using version specific APIs
     *
     * @param eventTime    - Microtime to use for event
     * @param podReference - ObjectReference pointing to rolled pod
     * @param reason       - reason the pod is being rolled
     * @param type         - the type of K8s event "Normal", or "Warning"
     * @param note         - the note to attach to the event
     */
    protected abstract void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note);

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
