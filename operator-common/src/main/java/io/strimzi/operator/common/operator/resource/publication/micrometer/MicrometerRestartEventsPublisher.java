package io.strimzi.operator.common.operator.resource.publication.micrometer;

import io.fabric8.kubernetes.api.model.Pod;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.RestartReason;
import io.strimzi.operator.common.model.RestartReasons;
import io.strimzi.operator.common.operator.resource.publication.RestartEventsPublisher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Locale;

public class MicrometerRestartEventsPublisher implements RestartEventsPublisher {

    private static final Logger log = LogManager.getLogger(MicrometerRestartEventsPublisher.class);
    private final MetricsProvider metricsProvider;
    private static final String description = "Pod restarts initiated by the Strimzi cluster operator";
    private static final String counterName = "strimzi.initiated.pod.restarts";

    public MicrometerRestartEventsPublisher(MetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    @Override
    public void publishRestartEvents(Pod pod, RestartReasons reasons) {
        Tag podName = Tag.of("pod.name", pod.getMetadata().getName());
        Tag podNamespace = Tag.of("pod.namespace", pod.getMetadata().getNamespace());

        for (RestartReason reason : reasons) {
            Tag restartReason = Tag.of("reason", reason.name().toLowerCase(Locale.ROOT));
            Tags tags = Tags.of(podName, podNamespace, restartReason);

            log.debug("Publishing Micrometer metric with name {}, tags, {}", counterName, tags);
            //Micrometer will return the existing counter if it already exists
            metricsProvider.counter(counterName, description, tags).increment();
        }

    }

}
