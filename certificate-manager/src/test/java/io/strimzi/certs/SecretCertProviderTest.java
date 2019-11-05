/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static java.util.Collections.emptyMap;

public class SecretCertProviderTest {

    private static CertManager ssl;
    private static SecretCertProvider secretCertProvider;
    private static OwnerReference ownerReference;

    @BeforeClass
    public static void before() {
        Assume.assumeTrue(System.getProperty("os.name").contains("nux"));
        ssl = new OpenSslCertManager();
        secretCertProvider = new SecretCertProvider();
        ownerReference = new OwnerReferenceBuilder()
                .withApiVersion("myapi/v1")
                .withKind("mykind")
                .withName("myname")
                .withUid("myuid")
                .build();
    }

    @Test
    public void testKeyAndCertInSecret() throws Exception {

        Base64.Decoder decoder = Base64.getDecoder();

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".str");

        ssl.generateSelfSignedCert(key, cert, 365);
        ssl.addCertToTrustStore(cert, "ca", store, "123456");

        Secret secret = secretCertProvider.createSecret("my-namespace", "my-secret",
                "ca.key", "ca.crt",
                key, cert,
                "truststore.p12", "truststore.password",
                store, "123456",
                emptyMap(), emptyMap(), ownerReference);

        assertEquals("my-secret", secret.getMetadata().getName());
        assertEquals("my-namespace", secret.getMetadata().getNamespace());
        assertEquals(1, secret.getMetadata().getOwnerReferences().size());
        assertEquals(ownerReference, secret.getMetadata().getOwnerReferences().get(0));
        assertEquals(4, secret.getData().size());
        assertTrue(Arrays.equals(Files.readAllBytes(key.toPath()), decoder.decode(secret.getData().get("ca.key"))));
        assertTrue(Arrays.equals(Files.readAllBytes(cert.toPath()), decoder.decode(secret.getData().get("ca.crt"))));
        assertTrue(Arrays.equals(Files.readAllBytes(store.toPath()), decoder.decode(secret.getData().get("truststore.p12"))));
        assertEquals("123456", new String(decoder.decode(secret.getData().get("truststore.password"))));

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testAddKeyAndCertInSecret() throws Exception {

        Base64.Decoder decoder = Base64.getDecoder();

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");

        ssl.generateSelfSignedCert(key, cert, 365);

        Secret secret = secretCertProvider.createSecret("my-namespace", "my-secret",
                "ca.key", "ca.crt",
                key, cert,
                null, null,
                null, null,
                emptyMap(), emptyMap(), ownerReference);

        File addedKey = File.createTempFile("added-key-", ".key");
        File addedCert = File.createTempFile("added-crt-", ".crt");

        ssl.generateSelfSignedCert(addedKey, addedCert, 365);

        secret = secretCertProvider.addSecret(secret, "added-key", "added-cert", addedKey, addedCert);

        assertEquals("my-secret", secret.getMetadata().getName());
        assertEquals("my-namespace", secret.getMetadata().getNamespace());
        assertEquals(1, secret.getMetadata().getOwnerReferences().size());
        assertEquals(ownerReference, secret.getMetadata().getOwnerReferences().get(0));
        assertEquals(4, secret.getData().size());
        assertTrue(Arrays.equals(Files.readAllBytes(key.toPath()), decoder.decode(secret.getData().get("ca.key"))));
        assertTrue(Arrays.equals(Files.readAllBytes(cert.toPath()), decoder.decode(secret.getData().get("ca.crt"))));
        assertTrue(Arrays.equals(Files.readAllBytes(addedKey.toPath()), decoder.decode(secret.getData().get("added-key"))));
        assertTrue(Arrays.equals(Files.readAllBytes(addedCert.toPath()), decoder.decode(secret.getData().get("added-cert"))));

        key.delete();
        cert.delete();
        addedKey.delete();
        addedCert.delete();
    }
}
