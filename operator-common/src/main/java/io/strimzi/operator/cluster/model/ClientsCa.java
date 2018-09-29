/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertManager;

public class ClientsCa extends Ca {
    public ClientsCa(CertManager certManager, String caCertSecretName, Secret clientsCaCert,
                     String caSecretKeyName, Secret clientsCaKey,
                     int validityDays, int renewalDays, boolean generateCa) {
        super(certManager, "clients-ca", caCertSecretName, clientsCaCert,
                caSecretKeyName, clientsCaKey,
                validityDays, renewalDays, generateCa);
    }

    @Override
    public String toString() {
        return "clients-ca";
    }
}
