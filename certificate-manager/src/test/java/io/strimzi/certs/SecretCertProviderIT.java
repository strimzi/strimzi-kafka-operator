/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Collections.emptyMap;

public class SecretCertProviderIT {

    private static CertManager ssl;
    private static SecretCertProvider secretCertProvider;
    private static OwnerReference ownerReference;

    @BeforeAll
    public static void before() {
        Assumptions.assumeTrue(System.getProperty("os.name").contains("nux"));
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

        File key = Files.createTempFile("key-", ".key").toFile();
        File cert = Files.createTempFile("crt-", ".crt").toFile();
        File store = Files.createTempFile("crt-", ".str").toFile();

        ssl.generateSelfSignedCert(key, cert, new Subject.Builder().withCommonName("Test CA").build(), 365);
        ssl.addCertToTrustStore(cert, "ca", store, "123456");

        Secret secret = secretCertProvider.createSecret("my-namespace", "my-secret",
                "ca.key", "ca.crt",
                key, cert,
                "ca.p12", "ca.password",
                store, "123456",
                emptyMap(), emptyMap(), ownerReference);

        assertThat(secret.getMetadata().getName(), is("my-secret"));
        assertThat(secret.getMetadata().getNamespace(), is("my-namespace"));
        assertThat(secret.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(secret.getMetadata().getOwnerReferences().get(0), is(ownerReference));
        assertThat(secret.getData().size(), is(4));
        assertThat(Arrays.equals(Files.readAllBytes(key.toPath()), decoder.decode(secret.getData().get("ca.key"))), is(true));
        assertThat(Arrays.equals(Files.readAllBytes(cert.toPath()), decoder.decode(secret.getData().get("ca.crt"))), is(true));
        assertThat(Arrays.equals(Files.readAllBytes(store.toPath()), decoder.decode(secret.getData().get("ca.p12"))), is(true));
        assertThat(new String(decoder.decode(secret.getData().get("ca.password"))), is("123456"));

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testAddKeyAndCertInSecret() throws Exception {

        Base64.Decoder decoder = Base64.getDecoder();

        File key = Files.createTempFile("key-", ".key").toFile();
        File cert = Files.createTempFile("crt-", ".crt").toFile();

        ssl.generateSelfSignedCert(key, cert, new Subject.Builder().withCommonName("Test CA").build(), 365);

        Secret secret = secretCertProvider.createSecret("my-namespace", "my-secret",
                "ca.key", "ca.crt",
                key, cert,
                null, null,
                null, null,
                emptyMap(), emptyMap(), ownerReference);

        File addedKey = Files.createTempFile("added-key-", ".key").toFile();
        File addedCert = Files.createTempFile("added-crt-", ".crt").toFile();

        ssl.generateSelfSignedCert(addedKey, addedCert, new Subject.Builder().withCommonName("Test CA").build(), 365);

        secret = secretCertProvider.addSecret(secret, "added-key", "added-cert", addedKey, addedCert);

        assertThat(secret.getMetadata().getName(), is("my-secret"));
        assertThat(secret.getMetadata().getNamespace(), is("my-namespace"));
        assertThat(secret.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(secret.getMetadata().getOwnerReferences().get(0), is(ownerReference));
        assertThat(secret.getData().size(), is(4));
        assertThat(Arrays.equals(Files.readAllBytes(key.toPath()), decoder.decode(secret.getData().get("ca.key"))), is(true));
        assertThat(Arrays.equals(Files.readAllBytes(cert.toPath()), decoder.decode(secret.getData().get("ca.crt"))), is(true));
        assertThat(Arrays.equals(Files.readAllBytes(addedKey.toPath()), decoder.decode(secret.getData().get("added-key"))), is(true));
        assertThat(Arrays.equals(Files.readAllBytes(addedCert.toPath()), decoder.decode(secret.getData().get("added-cert"))), is(true));

        key.delete();
        cert.delete();
        addedKey.delete();
        addedCert.delete();
    }
}
