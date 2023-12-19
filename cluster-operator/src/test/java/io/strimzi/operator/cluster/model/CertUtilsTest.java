/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CertUtilsTest {
    @Test
    public void testExistingCertificatesDiffer()   {
        Secret defaultSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret sameAsDefaultSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret scaleDownSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .build();

        Secret scaleUpSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .addToData("my-cluster-kafka-3.crt", "Certificate3")
                .addToData("my-cluster-kafka-3.key", "Key3")
                .addToData("my-cluster-kafka-4.crt", "Certificate4")
                .addToData("my-cluster-kafka-4.key", "Key4")
                .build();

        Secret changedSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "NewKey1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret changedScaleUpSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "NewCertificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .addToData("my-cluster-kafka-3.crt", "Certificate3")
                .addToData("my-cluster-kafka-3.key", "Key3")
                .addToData("my-cluster-kafka-4.crt", "Certificate4")
                .addToData("my-cluster-kafka-4.key", "Key4")
                .build();

        Secret changedScaleDownSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "NewCertificate0")
                .addToData("my-cluster-kafka-0.key", "NewKey0")
                .build();

        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, defaultSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, sameAsDefaultSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, scaleDownSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, scaleUpSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, changedSecret), is(true));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, changedScaleUpSecret), is(true));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, changedScaleDownSecret), is(true));
    }
}
