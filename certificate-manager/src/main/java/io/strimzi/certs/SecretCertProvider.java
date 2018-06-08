/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides Kubernetes secrets containing certificates
 */
public class SecretCertProvider {

    private static final String DEFAULT_KEY_KEY = "tls.key";
    private static final String DEFAULT_KEY_CERT = "tls.crt";


    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     * using default values for the keys in the Secret data section
     *
     * @param name Secret name
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @return the Secret
     * @throws IOException
     */
    public Secret createSecret(String name, File keyFile, File certFile) throws IOException {
        return createSecret(name, DEFAULT_KEY_KEY, DEFAULT_KEY_CERT, keyFile, certFile);
    }

    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     *
     * @param name Secret name
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @return the Secret
     * @throws IOException
     */
    public Secret createSecret(String name, String keyKey, String certKey, File keyFile, File certFile) throws IOException {

        Map<String, String> data = new HashMap<>();

        Base64.Encoder encoder = Base64.getEncoder();

        data.put(keyKey, encoder.encodeToString(Files.readAllBytes(keyFile.toPath())));
        data.put(certKey, encoder.encodeToString(Files.readAllBytes(certFile.toPath())));

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withData(data)
                .build();

        return secret;
    }
}
