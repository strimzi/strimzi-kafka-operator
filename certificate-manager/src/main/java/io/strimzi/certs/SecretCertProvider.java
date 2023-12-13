/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Provides Kubernetes secrets containing certificates
 */
public class SecretCertProvider {

    /**
     * A certificate entry in a ConfigMap. Each entry contains an entry name and data.
     */
    public enum SecretEntry {
        /**
         * A 64-bit encoded X509 Certificate
         */
        CRT(".crt"),
        /**
         * Entity private key
         */
        KEY(".key"),
        /**
         * Entity certificate and key as a P12 keystore
         */
        P12_KEYSTORE(".p12"),
        /**
         * P12 keystore password
         */
        P12_KEYSTORE_PASSWORD(".password");

        final String suffix;

        SecretEntry(String suffix) {
            this.suffix = suffix;
        }

        /**
         * @return The suffix of the entry name in the Secret
         */
        public String getSuffix() {
            return suffix;
        }

    }

    /**
     * Constructs a Map containing the provided certificates to be stored in a Kubernetes Secret.
     *
     * @param certificates to store
     * @return Map of certificate identifier to base64 encoded certificate or key
     */
    public static Map<String, String> buildSecretData(Map<String, CertAndKey> certificates) {
        Map<String, String> data = new HashMap<>(certificates.size() * 4);
        certificates.forEach((keyCertName, certAndKey) -> {
            data.put(keyCertName + SecretEntry.KEY.getSuffix(), certAndKey.keyAsBase64String());
            data.put(keyCertName + SecretEntry.CRT.getSuffix(), certAndKey.certAsBase64String());
            data.put(keyCertName + SecretEntry.P12_KEYSTORE.getSuffix(), certAndKey.keyStoreAsBase64String());
            data.put(keyCertName + SecretEntry.P12_KEYSTORE_PASSWORD.getSuffix(), certAndKey.storePasswordAsBase64String());
        });
        return data;
    }

    private static byte[] decodeFromSecret(Secret secret, String key) {
        if (secret.getData().get(key) != null && !secret.getData().get(key).isEmpty()) {
            return Base64.getDecoder().decode(secret.getData().get(key));
        } else {
            return new byte[]{};
        }
    }

    /**
     * Extracts the KeyStore from the Secret as a CertAndKey
     * @param secret to extract certificate and key from
     * @param keyCertName name of the KeyStore
     * @return the KeyStore as a CertAndKey. Returned object has empty truststore and
     * may have empty key, cert or keystore and null store password.
     */
    public static CertAndKey keyStoreCertAndKey(Secret secret, String keyCertName) {
        byte[] passwordBytes = decodeFromSecret(secret, keyCertName + SecretEntry.P12_KEYSTORE_PASSWORD.getSuffix());
        String password = passwordBytes.length == 0 ? null : new String(passwordBytes, StandardCharsets.US_ASCII);
        return new CertAndKey(
                decodeFromSecret(secret, keyCertName + SecretEntry.KEY.getSuffix()),
                decodeFromSecret(secret, keyCertName + SecretEntry.CRT.getSuffix()),
                new byte[]{},
                decodeFromSecret(secret, keyCertName + SecretEntry.P12_KEYSTORE.getSuffix()),
                password
        );
    }

    /**
     * Compares two Secrets with certificates and checks whether any value for a key which exists in both Secrets
     * changed. This method is used to evaluate whether rolling update of existing brokers is needed when secrets with
     * certificates change. It separates changes for existing certificates with other changes to the secret such as
     * added or removed certificates (scale-up or scale-down).
     *
     * @param current   Existing secret
     * @param desired   Desired secret
     *
     * @return  True if there is a key which exists in the data sections of both secrets and which changed.
     */
    public static boolean doExistingCertificatesDiffer(Secret current, Secret desired) {
        Map<String, String> currentData = current.getData();
        Map<String, String> desiredData = desired.getData();

        if (currentData == null) {
            return true;
        } else {
            for (Map.Entry<String, String> entry : currentData.entrySet()) {
                String desiredValue = desiredData.get(entry.getKey());
                if (entry.getValue() != null
                        && desiredValue != null
                        && !entry.getValue().equals(desiredValue)) {
                    return true;
                }
            }
        }

        return false;
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
     * @param storeKey key field in the Secret data section for the PKCS12 store
     * @param storePasswordKey key field in the Secret data section for the PKCS12 store password
     * @param storeFile PKCS12 store
     * @param storePassword PKCS12 store password
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     * @throws IOException If a file could not be read.
     */
    public Secret createSecret(String namespace, String name,
                               String keyKey, String certKey,
                               File keyFile, File certFile,
                               String storeKey, String storePasswordKey,
                               File storeFile, String storePassword,
                               Map<String, String> labels, Map<String, String> annotations,
                               OwnerReference ownerReference) throws IOException {
        byte[] key = Files.readAllBytes(keyFile.toPath());
        byte[] cert = Files.readAllBytes(certFile.toPath());
        byte[] store = null;
        if (storeFile != null) {
            store = Files.readAllBytes(storeFile.toPath());
        }
        byte[] password = storePassword != null ? storePassword.getBytes(StandardCharsets.US_ASCII) : null;


        return createSecret(namespace, name, keyKey, certKey, key, cert, storeKey, storePasswordKey, store, password, labels, annotations, ownerReference);
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
     * @param storeKey key field in the Secret data section for the PKCS12 store
     * @param storePasswordKey key field in the Secret data section for the PKCS12 store password
     * @param store PKCS12 store
     * @param storePassword PKCS12 store password
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     */
    public Secret createSecret(String namespace, String name,
                               String keyKey, String certKey,
                               byte[] key, byte[] cert,
                               String storeKey, String storePasswordKey,
                               byte[] store, byte[] storePassword,
                               Map<String, String> labels, Map<String, String> annotations,
                               OwnerReference ownerReference) {
        Map<String, String> data = new HashMap<>(4);

        Base64.Encoder encoder = Base64.getEncoder();

        data.put(keyKey, encoder.encodeToString(key));
        data.put(certKey, encoder.encodeToString(cert));
        if (store != null) {
            data.put(storeKey, encoder.encodeToString(store));
        }
        if (storePassword != null) {
            data.put(storePasswordKey, encoder.encodeToString(storePassword));
        }

        return createSecret(namespace, name, data, labels, annotations, ownerReference);
    }

    /**
     * Create a Kubernetes secret containing the provided secret data section
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param data Map with secret data / files
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     */
    public Secret createSecret(String namespace, String name, Map<String, String> data,
                               Map<String, String> labels, Map<String, String> annotations, OwnerReference ownerReference) {
        List<OwnerReference> or = ownerReference != null ? singletonList(ownerReference) : emptyList();
        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withAnnotations(annotations)
                    .withOwnerReferences(or)
                .endMetadata()
                .withType("Opaque")
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
     * @throws IOException If a file could not be read.
     */
    public Secret addSecret(Secret secret, String keyKey, String certKey, File keyFile, File certFile) throws IOException {
        byte[] key = Files.readAllBytes(keyFile.toPath());
        byte[] cert = Files.readAllBytes(certFile.toPath());

        return addSecret(secret, keyKey, certKey, key, cert);
    }
}
