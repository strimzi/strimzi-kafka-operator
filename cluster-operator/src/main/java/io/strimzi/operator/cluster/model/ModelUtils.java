/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;


import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.TlsCertificates;

import java.util.List;

public class ModelUtils {
    private ModelUtils() {}

    /**
     * Find the first secret in the given secrets with the given name
     */
    public static Secret findSecretWithName(List<Secret> secrets, String sname) {
        return secrets.stream().filter(s -> s.getMetadata().getName().equals(sname)).findFirst().orElse(null);
    }

    public static int getCertificateValidity(Kafka kafka) {
        int validity = AbstractModel.CERTS_EXPIRATION_DAYS;
        if (kafka.getSpec() != null) {
            validity = getCertificateValidity(kafka.getSpec().getTlsCertificates());
        }
        return validity;
    }

    public static int getCertificateValidity(TlsCertificates tlsCertificates) {
        int validity = AbstractModel.CERTS_EXPIRATION_DAYS;
        if (tlsCertificates != null
                && tlsCertificates.getValidityDays() > 0) {
            validity = tlsCertificates.getValidityDays();
        }
        return validity;
    }

    public static int getRenewalDays(TlsCertificates tlsCertificates) {
        return tlsCertificates != null ? tlsCertificates.getRenewalDays() : 30;
    }
}
