/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;

/**
 * Represents the identity used by the cluster operator during TLS client authentication in the PKCS12 format.
 * Can be used by clients that are unable to use the PEM format provided by PemAuthIdentity.
 */
public class ClusterOperatorPKCS12AuthIdentity {
    private static final String SECRET_KEY = "cluster-operator";
    private final byte[] keyStore;
    private final String password;

    /**
     * Constructs the ClusterOperatorPKCS12AuthIdentity.
     * @param secret Kubernetes Secret containing the client authentication identity
     */
    public ClusterOperatorPKCS12AuthIdentity(Secret secret) {
        keyStore = Util.decodeBase64FieldFromSecret(secret, String.format("%s.p12", SECRET_KEY));
        password = Util.asciiFieldFromSecret(secret, String.format("%s.password", SECRET_KEY));
    }

    /**
     * @return The KeyStore for this authentication identity as a byte array
     */
    public byte[] keystore() {
        return keyStore;
    }

    /**
     * @return The KeyStore password for this authentication identity as a String
     */
    public String password() {
        return password;
    }
}
