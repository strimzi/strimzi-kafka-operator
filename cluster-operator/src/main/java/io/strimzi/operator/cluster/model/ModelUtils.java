/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;


import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateAuthority;

import java.util.List;

public class ModelUtils {
    private ModelUtils() {}

    /**
     * Find the first secret in the given secrets with the given name
     */
    public static Secret findSecretWithName(List<Secret> secrets, String sname) {
        return secrets.stream().filter(s -> s.getMetadata().getName().equals(sname)).findFirst().orElse(null);
    }

    public static int getCertificateValidity(CertificateAuthority certificateAuthority) {
        int validity = AbstractModel.CERTS_EXPIRATION_DAYS;
        if (certificateAuthority != null
                && certificateAuthority.getValidityDays() > 0) {
            validity = certificateAuthority.getValidityDays();
        }
        return validity;
    }

    public static int getRenewalDays(CertificateAuthority certificateAuthority) {
        return certificateAuthority != null ? certificateAuthority.getRenewalDays() : 30;
    }
}
