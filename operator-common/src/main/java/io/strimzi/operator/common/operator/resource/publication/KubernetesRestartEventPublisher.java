package io.strimzi.operator.common.operator.resource.publication;

import com.fasterxml.jackson.annotation.JsonValue;
import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.model.RestartReasons;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class KubernetesRestartEventPublisher {
    public KubernetesRestartEventPublisher(KubernetesClient kubernetesClient, String operatorId) {

    }

    public void publishRestartEvent(Pod restartingPod, RestartReasons reasons) {

    }

    /**
     * Works around an issue in fabric8 where MicroTime serialises incorrectly and is rejected by the API server
     * @see <a href="https://github.com/fabric8io/kubernetes-client/issues/3240">Relevant issue</a>
     */
    public static class WorkaroundMicroTime extends MicroTime {

        private static final DateTimeFormatter k8sMicroTime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSSSSSXXX");
        private final String k8sFormattedMicroTime;

        public WorkaroundMicroTime(ZonedDateTime dateTime) {
            this.k8sFormattedMicroTime = k8sMicroTime.format(dateTime);
        }

        @JsonValue
        public String serialise() {
            return this.k8sFormattedMicroTime;
        }
    }
}
