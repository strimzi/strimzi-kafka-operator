/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.Secret;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;

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

        Secret secret = secretCertProvider.createSecret("secret", key, cert);

        assertEquals("secret", secret.getMetadata().getName());
        assertEquals(2, secret.getData().size());
        assertTrue(Arrays.equals(readFileToByteArray(key), decoder.decode(secret.getData().get("tls.key"))));
        assertTrue(Arrays.equals(readFileToByteArray(cert), decoder.decode(secret.getData().get("tls.crt"))));

        key.delete();
        cert.delete();
    }

    private byte[] readFileToByteArray(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(fis);
        byte[] bytes = new byte[(int) file.length()];
        dis.readFully(bytes);
        dis.close();
        return bytes;
    }
}
