/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CertificateExpirationPolicy {

    @JsonProperty("renew-certificate")
    RENEW_CERTIFICATE,

    @JsonProperty("replace-key")
    REPLACE_KEY
}
