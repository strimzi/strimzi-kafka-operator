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

    public static final String DEFAULT_KEY_KEY = "tls.key";
    public static final String DEFAULT_KEY_CERT = "tls.crt";


    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     * using default values for the keys in the Secret data section
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @param labels Labels to add to the Secret
     * @return the Secret
     * @throws IOException
     */
    public Secret createSecret(String namespace, String name, File keyFile, File certFile, Map<String, String> labels) throws IOException {
        return createSecret(namespace, name, DEFAULT_KEY_KEY, DEFAULT_KEY_CERT, keyFile, certFile, labels);
    }

    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @param labels Labels to add to the Secret
     * @return the Secret
     * @throws IOException
     */
    public Secret createSecret(String namespace, String name, String keyKey, String certKey, File keyFile, File certFile, Map<String, String> labels) throws IOException {
        byte[] key = Files.readAllBytes(keyFile.toPath());
        byte[] cert = Files.readAllBytes(certFile.toPath());

        return createSecret(namespace, name, keyKey, certKey, key, cert, labels);
    }

    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param key private key to store
     * @param cert certificate to store
     * @param labels Labels to add to the Secret
     * @return the Secret
     * @throws IOException
     */
    public Secret createSecret(String namespace, String name, String keyKey, String certKey, byte[] key, byte[] cert, Map<String, String> labels) {
        Map<String, String> data = new HashMap<>();

        Base64.Encoder encoder = Base64.getEncoder();

        data.put(keyKey, encoder.encodeToString(key));
        data.put(certKey, encoder.encodeToString(cert));

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withData(data)
                .build();

        return secret;
    }

    /**
     * Add a data value to an existing Secret
     *
     * @param secret Secret
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param key private key to store
     * @param cert certificate to store
     * @return the Secret
     * @throws IOException
     */
    public Secret addSecret(Secret secret, String keyKey, String certKey, byte[] key, byte[] cert) {
        Base64.Encoder encoder = Base64.getEncoder();

        secret.getData().put(keyKey, encoder.encodeToString(key));
        secret.getData().put(certKey, encoder.encodeToString(cert));

        return secret;
    }

    /**
     * Add a data value to an existing Secret
     *
     * @param secret Secret
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @return the Secret
     * @throws IOException
     */
    public Secret addSecret(Secret secret, String keyKey, String certKey, File keyFile, File certFile) throws IOException {
        byte[] key = Files.readAllBytes(keyFile.toPath());
        byte[] cert = Files.readAllBytes(certFile.toPath());

        return addSecret(secret, keyKey, certKey, key, cert);
    }
}
