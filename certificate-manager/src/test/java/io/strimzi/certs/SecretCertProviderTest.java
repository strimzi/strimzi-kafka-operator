/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.Secret;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SecretCertProviderTest {

    private static CertManager ssl;
    private static SecretCertProvider secretCertProvider;

    @BeforeClass
    public static void before() {
        ssl = new OpenSslCertManager();
        secretCertProvider = new SecretCertProvider();
    }

    @Test
    public void testKeyAndCertInSecret() throws IOException {

        Base64.Decoder decoder = Base64.getDecoder();

        File key = File.createTempFile("tls", "key");
        File cert = File.createTempFile("tls", "crt");

        ssl.generateSelfSignedCert(key, cert, 365);

        Secret secret = secretCertProvider.createSecret("my-namespace", "my-secret", key, cert, Collections.emptyMap());

        assertEquals("my-secret", secret.getMetadata().getName());
        assertEquals("my-namespace", secret.getMetadata().getNamespace());
        assertEquals(2, secret.getData().size());
        assertTrue(Arrays.equals(Files.readAllBytes(key.toPath()), decoder.decode(secret.getData().get("tls.key"))));
        assertTrue(Arrays.equals(Files.readAllBytes(cert.toPath()), decoder.decode(secret.getData().get("tls.crt"))));

        key.delete();
        cert.delete();
    }

    @Test
    public void testAddKeyAndCertInSecret() throws IOException {

        Base64.Decoder decoder = Base64.getDecoder();

        File key = File.createTempFile("tls", "key");
        File cert = File.createTempFile("tls", "crt");

        ssl.generateSelfSignedCert(key, cert, 365);

        Secret secret = secretCertProvider.createSecret("my-namespace", "my-secret", key, cert, Collections.emptyMap());

        File addedKey = File.createTempFile("tls", "key");
        File addedCert = File.createTempFile("tls", "crt");

        ssl.generateSelfSignedCert(addedKey, addedCert, 365);

        secret = secretCertProvider.addSecret(secret, "added-key", "added-cert", addedKey, addedCert);

        assertEquals("my-secret", secret.getMetadata().getName());
        assertEquals("my-namespace", secret.getMetadata().getNamespace());
        assertEquals(4, secret.getData().size());
        assertTrue(Arrays.equals(Files.readAllBytes(key.toPath()), decoder.decode(secret.getData().get("tls.key"))));
        assertTrue(Arrays.equals(Files.readAllBytes(cert.toPath()), decoder.decode(secret.getData().get("tls.crt"))));
        assertTrue(Arrays.equals(Files.readAllBytes(addedKey.toPath()), decoder.decode(secret.getData().get("added-key"))));
        assertTrue(Arrays.equals(Files.readAllBytes(addedCert.toPath()), decoder.decode(secret.getData().get("added-cert"))));

        key.delete();
        cert.delete();
        addedKey.delete();
        addedCert.delete();
    }
}
