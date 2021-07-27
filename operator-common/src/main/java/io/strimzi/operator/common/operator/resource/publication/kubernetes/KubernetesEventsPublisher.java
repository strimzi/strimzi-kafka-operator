package io.strimzi.operator.common.operator.resource.publication.kubernetes;

import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.PlatformFeaturesAvailability.EventApiVersion;
import io.strimzi.operator.common.model.RestartReason;
import io.strimzi.operator.common.model.RestartReasons;
import io.strimzi.operator.common.operator.resource.publication.RestartEventsPublisher;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.versions.CoreEventPublisher;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.versions.V1Beta1EventPublisher;
import io.strimzi.operator.common.operator.resource.publication.kubernetes.versions.V1EventPublisher;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class KubernetesEventsPublisher implements RestartEventsPublisher {

    private final Clock clock;

    // So far, the only restart reasons that qualify for a K8s "Warning", as opposed to "Normal" level events
    Set<RestartReason> warningReasons = Set.of(RestartReason.ADMIN_CLIENT_CANNOT_CONNECT_TO_BROKER, RestartReason.POD_FORCE_ROLL_ON_ERROR);

    protected String action = "StrimziInitiatedPodRestart";
    protected String controller = "strimzi.io/cluster-operator";

    // Matches first character or characters following an underscore
    private static final Pattern pascalCaseHelper = Pattern.compile("^.|_.");

    private static final int maxMessageLength = 1000;
    private static final String diaresis = "...";

    public KubernetesEventsPublisher(Clock clock) {
        this.clock = clock;
    }

    public KubernetesEventsPublisher() {
        this(Clock.systemDefaultZone());
    }

    /**
     * Create an instance for the highest event API, there's three possible subtypes, due to how fabric8 delineates the differing
     * event APIs.
     * @param client - Kubernetes client
     * @param operatorId - the id of the current cluster operator instance
     * @param highestEventApiVersion - the highest supported event API detected in {@link io.strimzi.operator.PlatformFeaturesAvailability}
     * @return instance of the appropriate publisher for the given API
     */
    public static RestartEventsPublisher createPublisher(KubernetesClient client, String operatorId, EventApiVersion highestEventApiVersion) {
        Clock clock = Clock.systemDefaultZone();
        if (highestEventApiVersion == EventApiVersion.V1) {
            return new V1EventPublisher(clock, client, operatorId);
        } else if (highestEventApiVersion == EventApiVersion.V1BETA1) {
            return new V1Beta1EventPublisher(clock, client, operatorId);
        }
        else {
            return new CoreEventPublisher(clock, client, operatorId);
        }
    }

    @Override
    public void publishRestartEvents(Pod pod, RestartReasons reasons) {
        MicroTime k8sEventTime = new WorkaroundMicroTime(ZonedDateTime.now(clock));
        ObjectReference podReference = createPodReference(pod);

        for (RestartReason reason : reasons) {
            String message = maybeTruncated(reasons.getNoteFor(reason));
            String type = getType(reason);
            String k8sFormattedReason = pascalCasedReason(reason);
            publishEvent(k8sEventTime, podReference, k8sFormattedReason, type, message);
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

    // GoLovesPascalCaseSoKubernetesDoesToo
    String pascalCasedReason(RestartReason reason) {
        Matcher matcher = pascalCaseHelper.matcher(reason.name().toLowerCase(Locale.ROOT));
        return matcher.replaceAll(result -> result.group().replace("_", "").toUpperCase(Locale.ROOT));
    }

    ObjectReference createPodReference(Pod pod) {
        // I'm uncertain as to whether or not RV should be included, as would likely prevent event aggregation, as
        // it would make the ObjectReference for Regarding differ each time. Copy/paste from event api design docs..
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
     * Kubernetes only supports two kinds of event types currently - Warning, and Normal.
     * @param reason the reason for restart
     * @return Warning or Normal, which is determined based on an explicit set of restart reasons that are abnormal
     */
    String getType(RestartReason reason) {
        return warningReasons.contains(reason) ? "Warning" : "Normal";
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
        } else if (stringBytes.length <= maxMessageLength)  {
            return note;
        } else {
            return new String(stringBytes, 0, 997, UTF_8) + diaresis;
        }
    }
}
