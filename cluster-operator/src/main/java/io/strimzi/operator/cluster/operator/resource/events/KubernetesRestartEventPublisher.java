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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Publishes KafkaRoller restart events as Kubernetes events, using either the events.k8s.io/v1beta1 or v1 API,
 * depending on which is available.
 */
public abstract class KubernetesRestartEventPublisher {

    private static final Logger LOG = LogManager.getLogger(KubernetesRestartEventPublisher.class);

    private final Clock clock;

    protected String action = "StrimziInitiatedPodRestart";
    protected String controller = "strimzi.io/cluster-operator";

    // Matches first character or characters following an underscore
    private static final Pattern PASCAL_CASE_HELPER = Pattern.compile("^.|_.");

    // K8s events are required to have a message of 1KiB or smaller
    private static final int MAX_MESSAGE_LENGTH = 1000;
    private static final String DIARESIS = "...";

    public KubernetesRestartEventPublisher() {
        this(Clock.systemDefaultZone());
    }

    protected KubernetesRestartEventPublisher(Clock clock) {
        this.clock = clock;
    }

    /**
     * Create an instance for the highest event API, there's three possible subtypes, due to how fabric8 delineates the differing
     * event APIs.
     * @param client Kubernetes client
     * @param operatorName the pod name of the current cluster operator instance
     * @param hasEventsV1 if the cluster is using events.k8s.io/v1 instead of events.k8s.io/v1beta1
     * @return instance of the appropriate publisher for the given API
     */
    public static KubernetesRestartEventPublisher createPublisher(KubernetesClient client, String operatorName, boolean hasEventsV1) {
        Clock clock = Clock.systemDefaultZone();
        if (hasEventsV1) {
            return new V1EventPublisher(clock, client, operatorName);
        } else {
            return new V1Beta1EventPublisher(clock, client, operatorName);
        }
    }

    public void publishRestartEvents(Pod pod, RestartReasons reasons) {
        MicroTime k8sEventTime = new WorkaroundMicroTime(ZonedDateTime.now(clock));
        ObjectReference podReference = createPodReference(pod);

        try {
            for (RestartReason reason : reasons) {
                String note = maybeTruncated(reasons.getNoteFor(reason));
                String type = "Normal";
                String k8sFormattedReason = pascalCasedReason(reason);
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
     * @param eventTime - Microtime to use for event
     * @param podReference - ObjectReference pointing to rolled pod
     * @param reason - reason the pod is being rolled
     * @param type - the type of K8s event "Normal", or "Warning"
     * @param note - the note to attach to the event
     */
    protected abstract void publishEvent(MicroTime eventTime, ObjectReference podReference, String reason, String type, String note);

    // Go loves PascalCase so Kubernetes does too
    String pascalCasedReason(RestartReason reason) {
        Matcher matcher = PASCAL_CASE_HELPER.matcher(reason.name().toLowerCase(Locale.ROOT));
        return matcher.replaceAll(result -> result.group().replace("_", "").toUpperCase(Locale.ROOT));
    }

    ObjectReference createPodReference(Pod pod) {
        // I'm uncertain whether RV should be included, as would likely prevent event aggregation, as
        // it would make the ObjectReference for Regarding different each time. Copy/paste from event api design docs...
        // > The assumption we make for deduplication logic after API changes is that Events with the
        // > same <Regarding, Action, Reason, ReportingController, ReportingInstance, Related> tuples are considered isomorphic.
        // > This allows us to define notion of "event series", which is series of isomorphic events happening not farther away from each other than some defined threshold.
        // > E.g. Events happening every second are considered a series, but Events happening every hour are not.
        return new ObjectReferenceBuilder().withKind("Pod")
                                           .withNamespace(pod.getMetadata().getNamespace())
                                           .withName(pod.getMetadata().getName())
                                           //.withResourceVersion(pod.getMetadata().getResourceVersion())
                                           .build();
    }


    /**
     * While the core event API doesn't set a limit on note sizes, events.k8s.io/v1beta1 and v1 do, which is 1kB.
     * It's a reasonably safe bet that notes will only include characters in the ASCII subset, so that's all that is supported.
     * An exception is thrown if multi-byte characters appear in the note.
     *
     * @param note the candidate string for truncation
     * @return the note unchanged if <=1kB, truncated otherwise, with the last 3 characters being "..." to indicate truncation
     * @throws UnsupportedOperationException if the note contains multi-byte characters when represented as UTF-8
     */
    String maybeTruncated(String note) throws UnsupportedOperationException {
        byte[] stringBytes = note.getBytes(UTF_8);

        if (note.length() != stringBytes.length) {
            throw new UnsupportedOperationException("Truncating messages containing multibyte characters isn't implemented");
        } else if (stringBytes.length <= MAX_MESSAGE_LENGTH)  {
            return note;
        } else {
            return new String(stringBytes, 0, 997, UTF_8) + DIARESIS;
        }
    }
}
