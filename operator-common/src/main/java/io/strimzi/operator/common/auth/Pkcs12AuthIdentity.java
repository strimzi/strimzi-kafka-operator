/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;

/**
 * Represents the identity used during TLS client authentication in the PKCS12 format.
 * Can be used by clients that are unable to use the PEM format provided by PemAuthIdentity.
 */
public class Pkcs12AuthIdentity {
    private final byte[] keyStore;
    private final String password;

    /**
     * Constructs the Pkcs12AuthIdentity.
     * @param secret Kubernetes Secret containing the client authentication identity
     * @param secretCertName Key in the Kubernetes Secret that is associated with the requested identity
     */
    private Pkcs12AuthIdentity(Secret secret, String secretCertName) {
        keyStore = Util.decodeBase64FieldFromSecret(secret, String.format("%s.p12", secretCertName));
        password = Util.asciiFieldFromSecret(secret, String.format("%s.password", secretCertName));
    }

    /**
     * Returns the instance of Pkcs12AuthIdentity that represents the identity of the
     * cluster operator during TLS client authentication. This also validates the provided
     * Secret contains a valid certificate chain.
     *
     * @param secret Kubernetes Secret containing the client authentication identity
     *
     * @return Pkcs12AuthIdentity to use as the client authentication identity during TLS authentication
     */
    public static Pkcs12AuthIdentity clusterOperator(Secret secret) {
        return new Pkcs12AuthIdentity(secret, "cluster-operator");
    }

    /**
     * Authentication identity as a KeyStore file for clients to use during TlS connections.
     * File password can be retrieved using password().
     * @return The KeyStore for this authentication identity as a byte array
     */
    public byte[] keystore() {
        return keyStore;
    }

    /**
     * Password for the corresponding KeyStore file provided by keystore().
     * @return The KeyStore password for this authentication identity as a String
     */
    public String password() {
        return password;
    }
}
