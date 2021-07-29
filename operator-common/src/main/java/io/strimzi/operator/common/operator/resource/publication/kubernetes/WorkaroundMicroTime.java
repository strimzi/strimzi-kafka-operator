/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.publication.kubernetes;

import com.fasterxml.jackson.annotation.JsonValue;
import io.fabric8.kubernetes.api.model.MicroTime;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Works around an issue in fabric8 where MicroTime serialises incorrectly and is rejected by the API server
 * I'm not sure if it's overly precise in how it formats timezones and microseconds for the Go server consuming this code,
 *
 * @see <a href="https://github.com/fabric8io/kubernetes-client/issues/3240">Relevant issue</a>
 */
public class WorkaroundMicroTime extends MicroTime {

    private static final DateTimeFormatter K8S_MICROTIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSSSSSXXX");

    public WorkaroundMicroTime(ZonedDateTime dateTime) {
        setTime(K8S_MICROTIME.format(dateTime));
    }

    @JsonValue
    public String serialise() {
        return getTime();
    }
}
