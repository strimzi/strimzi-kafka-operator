/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void testTrustedCertificatesVolumes()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<Volume> volumes = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumes(volumes, List.of(cert1, cert2, cert3), false);

        assertThat(volumes.size(), is(2));
        assertThat(volumes.get(0).getName(), is("first-certificate"));
        assertThat(volumes.get(0).getSecret().getSecretName(), is("first-certificate"));
        assertThat(volumes.get(1).getName(), is("second-certificate"));
        assertThat(volumes.get(1).getSecret().getSecretName(), is("second-certificate"));
    }

    @Test
    public void testTrustedCertificatesVolumesWithPrefix()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<Volume> volumes = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumes(volumes, List.of(cert1, cert2, cert3), false, "prefixed");

        assertThat(volumes.size(), is(2));
        assertThat(volumes.get(0).getName(), is("prefixed-first-certificate"));
        assertThat(volumes.get(0).getSecret().getSecretName(), is("first-certificate"));
        assertThat(volumes.get(1).getName(), is("prefixed-second-certificate"));
        assertThat(volumes.get(1).getSecret().getSecretName(), is("second-certificate"));
    }

    @Test
    public void testTrustedCertificatesVolumeMountsWithPrefix()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<VolumeMount> mounts = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumeMounts(mounts, List.of(cert1, cert2, cert3), "/my/path/", "prefixed");

        assertThat(mounts.size(), is(2));
        assertThat(mounts.get(0).getName(), is("prefixed-first-certificate"));
        assertThat(mounts.get(0).getMountPath(), is("/my/path/first-certificate"));
        assertThat(mounts.get(1).getName(), is("prefixed-second-certificate"));
        assertThat(mounts.get(1).getMountPath(), is("/my/path/second-certificate"));
    }

    @Test
    public void testTrustedCertificatesVolumeMounts()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<VolumeMount> mounts = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumeMounts(mounts, List.of(cert1, cert2, cert3), "/my/path/");

        assertThat(mounts.size(), is(2));
        assertThat(mounts.get(0).getName(), is("first-certificate"));
        assertThat(mounts.get(0).getMountPath(), is("/my/path/first-certificate"));
        assertThat(mounts.get(1).getName(), is("second-certificate"));
        assertThat(mounts.get(1).getMountPath(), is("/my/path/second-certificate"));
    }

    @Test
    public void testTrustedCertsEnvVar()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        CertSecretSource cert4 = new CertSecretSourceBuilder()
                .withSecretName("third-certificate")
                .withCertificate("*.crt")
                .build();

        CertSecretSource cert5 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("*.pem")
                .build();

        assertThat(CertUtils.trustedCertsEnvVar(List.of(cert1, cert2, cert3, cert4, cert5)), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt;third-certificate/*.crt;first-certificate/*.pem"));
    }
}
