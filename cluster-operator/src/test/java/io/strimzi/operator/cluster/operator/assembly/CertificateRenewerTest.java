/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.model.CertificateRenewer;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CertificateRenewerTest {

    final static String SECRET_NAME = "secret-name";
    final static String NAMESPACE = "my-namespace";
    final static String COMPONENT_COMMON_NAME = "my-component-common-name";
    final static String KEY_CERT_NAME = "my-component-key-cert-name";


    @Test
    public void testRenewalOfCertificateWithNullSecret() throws IOException {
        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();
        boolean isMaintenanceTimeWindowsSatisfied = true;

        Secret newSecret = CertificateRenewer.of(clusterCaMock, COMPONENT_COMMON_NAME, KEY_CERT_NAME, isMaintenanceTimeWindowsSatisfied)
                .signedCertificateSecret(null, SECRET_NAME, NAMESPACE, labels, ownerReference);

        assertThat(newSecret.getData(), allOf(
                hasEntry(KEY_CERT_NAME + ".crt", newCertAndKey.certAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".key", newCertAndKey.keyAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".p12", newCertAndKey.keyStoreAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".password", newCertAndKey.storePasswordAsBase64String())
        ));
    }

    @Test
    public void testRenewalOfCertificateWithRenewingCa() throws IOException {
        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                    .withNewName("test-secret")
                .endMetadata()
                .addToData(KEY_CERT_NAME + ".crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData(KEY_CERT_NAME + ".key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData(KEY_CERT_NAME + ".p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData(KEY_CERT_NAME + ".password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.certRenewed()).thenReturn(true);
        when(clusterCaMock.isExpiring(any(), any())).thenReturn(false);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();
        boolean isMaintenanceTimeWindowsSatisfied = true;

        Secret newSecret = CertificateRenewer.of(clusterCaMock, COMPONENT_COMMON_NAME, KEY_CERT_NAME, isMaintenanceTimeWindowsSatisfied)
                .signedCertificateSecret(initialSecret, SECRET_NAME, NAMESPACE, labels, ownerReference);

        assertThat(newSecret.getData(), allOf(
                hasEntry(KEY_CERT_NAME + ".crt", newCertAndKey.certAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".key", newCertAndKey.keyAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".p12", newCertAndKey.keyStoreAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".password", newCertAndKey.storePasswordAsBase64String())
        ));
    }

    @Test
    public void testRenewalOfCertificateDelayedRenewal() throws IOException {
        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                .withNewName("test-secret")
                .endMetadata()
                .addToData(KEY_CERT_NAME + ".crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData(KEY_CERT_NAME + ".key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData(KEY_CERT_NAME + ".p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData(KEY_CERT_NAME + ".password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.certRenewed()).thenReturn(false);
        when(clusterCaMock.isExpiring(any(), any())).thenReturn(true);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();
        boolean isMaintenanceTimeWindowsSatisfied = true;

        Secret newSecret = CertificateRenewer.of(clusterCaMock, COMPONENT_COMMON_NAME, KEY_CERT_NAME, isMaintenanceTimeWindowsSatisfied)
                .signedCertificateSecret(initialSecret, SECRET_NAME, NAMESPACE, labels, ownerReference);

        assertThat(newSecret.getData(), allOf(
                hasEntry(KEY_CERT_NAME + ".crt", newCertAndKey.certAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".key", newCertAndKey.keyAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".p12", newCertAndKey.keyStoreAsBase64String()),
                hasEntry(KEY_CERT_NAME + ".password", newCertAndKey.storePasswordAsBase64String())
        ));
    }

    @Test
    public void testRenewalOfCertificateDelayedRenewalOutsideOfMaintenanceWindow() throws IOException {
        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                    .withNewName("test-secret")
                .endMetadata()
                .addToData(KEY_CERT_NAME + ".crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData(KEY_CERT_NAME + ".key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData(KEY_CERT_NAME + ".p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData(KEY_CERT_NAME + ".password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        CertAndKey newCertAndKey = new CertAndKey("new-key".getBytes(), "new-cert".getBytes(), "new-truststore".getBytes(), "new-keystore".getBytes(), "new-password");
        ClusterCa clusterCaMock = mock(ClusterCa.class);
        when(clusterCaMock.certRenewed()).thenReturn(false);
        when(clusterCaMock.isExpiring(any(), any())).thenReturn(true);
        when(clusterCaMock.generateSignedCert(anyString(), anyString())).thenReturn(newCertAndKey);
        Labels labels = Labels.forStrimziCluster("my-cluster");
        OwnerReference ownerReference = new OwnerReference();
        boolean isMaintenanceTimeWindowsSatisfied = false;

        Secret newSecret = CertificateRenewer.of(clusterCaMock, COMPONENT_COMMON_NAME, KEY_CERT_NAME, isMaintenanceTimeWindowsSatisfied)
                .signedCertificateSecret(initialSecret, SECRET_NAME, NAMESPACE, labels, ownerReference);

        assertThat(newSecret.getData(), allOf(
                hasEntry(KEY_CERT_NAME + ".crt", Base64.getEncoder().encodeToString("old-cert".getBytes())),
                hasEntry(KEY_CERT_NAME + ".key", Base64.getEncoder().encodeToString("old-key".getBytes())),
                hasEntry(KEY_CERT_NAME + ".p12", Base64.getEncoder().encodeToString("old-keystore".getBytes())),
                hasEntry(KEY_CERT_NAME + ".password", Base64.getEncoder().encodeToString("old-password".getBytes()))
        ));
    }
}
