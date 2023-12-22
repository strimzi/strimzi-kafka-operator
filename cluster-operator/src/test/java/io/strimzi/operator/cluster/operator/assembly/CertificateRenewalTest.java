/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static io.strimzi.operator.common.model.Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CertificateRenewalTest {
    @Test
    public void testRenewalOfDeploymentCertificatesWithNullSecret() throws IOException {
        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        when(clusterCaMock.caCertGenerationFullAnnotation()).thenReturn(Map.entry(ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
        String namespace = "my-namespace";
        String secretName = "my-secret";
        String commonName = "deployment";
        String keyCertName = "deployment";
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();

        Secret newSecret = CertUtils.buildTrustedCertificateSecret(Reconciliation.DUMMY_RECONCILIATION, clusterCaMock, null, namespace, secretName, commonName,
                keyCertName, labels, ownerReference, true);

        assertThat(newSecret.getData(), hasEntry("deployment.crt", newCertAndKey.certAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.key", newCertAndKey.keyAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.p12", newCertAndKey.keyStoreAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.password", newCertAndKey.storePasswordAsBase64String()));
    }

    @Test
    public void testRenewalOfDeploymentCertificatesWithRenewingCa() throws IOException {
        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("test-secret")
                .endMetadata()
                .addToData("deployment.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("deployment.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("deployment.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("deployment.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.certRenewed()).thenReturn(true);
        when(clusterCaMock.isExpiring(any(), any())).thenReturn(false);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        when(clusterCaMock.caCertGenerationFullAnnotation()).thenReturn(Map.entry(ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
        String namespace = "my-namespace";
        String secretName = "my-secret";
        String commonName = "deployment";
        String keyCertName = "deployment";
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();

        Secret newSecret = CertUtils.buildTrustedCertificateSecret(Reconciliation.DUMMY_RECONCILIATION, clusterCaMock, initialSecret, namespace, secretName, commonName,
                keyCertName, labels, ownerReference, true);

        assertThat(newSecret.getData(), hasEntry("deployment.crt", newCertAndKey.certAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.key", newCertAndKey.keyAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.p12", newCertAndKey.keyStoreAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.password", newCertAndKey.storePasswordAsBase64String()));
    }

    @Test
    public void testRenewalOfDeploymentCertificatesDelayedRenewal() throws IOException {
        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("test-secret")
                .endMetadata()
                .addToData("deployment.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("deployment.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("deployment.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("deployment.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.certRenewed()).thenReturn(false);
        when(clusterCaMock.isExpiring(any(), any())).thenReturn(true);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        when(clusterCaMock.caCertGenerationFullAnnotation()).thenReturn(Map.entry(ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
        String namespace = "my-namespace";
        String secretName = "my-secret";
        String commonName = "deployment";
        String keyCertName = "deployment";
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();

        Secret newSecret = CertUtils.buildTrustedCertificateSecret(Reconciliation.DUMMY_RECONCILIATION, clusterCaMock, initialSecret, namespace, secretName, commonName,
                keyCertName, labels, ownerReference, true);

        assertThat(newSecret.getData(), hasEntry("deployment.crt", newCertAndKey.certAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.key", newCertAndKey.keyAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.p12", newCertAndKey.keyStoreAsBase64String()));
        assertThat(newSecret.getData(), hasEntry("deployment.password", newCertAndKey.storePasswordAsBase64String()));
    }

    @Test
    public void testRenewalOfDeploymentCertificatesDelayedRenewalOutsideOfMaintenanceWindow() throws IOException {
        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("test-secret")
                .endMetadata()
                .addToData("deployment.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("deployment.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("deployment.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("deployment.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.certRenewed()).thenReturn(false);
        when(clusterCaMock.isExpiring(any(), any())).thenReturn(true);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        when(clusterCaMock.caCertGenerationFullAnnotation()).thenReturn(Map.entry(ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
        String namespace = "my-namespace";
        String secretName = "my-secret";
        String commonName = "deployment";
        String keyCertName = "deployment";
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();

        Secret newSecret = CertUtils.buildTrustedCertificateSecret(Reconciliation.DUMMY_RECONCILIATION, clusterCaMock, initialSecret, namespace, secretName, commonName,
                keyCertName, labels, ownerReference, false);

        assertThat(newSecret.getData(), hasEntry("deployment.crt", Base64.getEncoder().encodeToString("old-cert".getBytes())));
        assertThat(newSecret.getData(), hasEntry("deployment.key", Base64.getEncoder().encodeToString("old-key".getBytes())));
        assertThat(newSecret.getData(), hasEntry("deployment.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes())));
        assertThat(newSecret.getData(), hasEntry("deployment.password", Base64.getEncoder().encodeToString("old-password".getBytes())));
    }
}