/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.CertManagerCa;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.strimzi.operator.common.ca.Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION;
import static io.strimzi.operator.common.ca.Ca.CA_CRT;

/**
 * CA provider for cert-manager integration.
 * Manages CAs using external cert-manager for certificate issuance.
 */
public class CertManagerCaProvider extends CaProvider {
    private final CertificateAuthority certificateAuthority;
    private final Secret clusterOperatorSecret;
    private final CertManagerCertificateOperator certificateOperator;
    private final SecretOperator secretOperator;

    /**
     * Constructor.
     *
     * @param reconciliation            Reconciliation marker
     * @param caRole                    The role of this CA
     * @param caConfig                  CA configuration
     * @param kafkaCr                   The Kafka custom resource
     * @param existingCaCertSecret      Existing CA certificate secret
     * @param clusterOperatorSecret     Cluster operator secret
     * @param certificateOperator       Certificate operator for managing cert-manager certificates
     * @param secretOperator            Secret operator for managing secrets
     */
    public CertManagerCaProvider(Reconciliation reconciliation,
                                 Ca.CaRole caRole,
                                 CaConfig caConfig,
                                 Kafka kafkaCr,
                                 Secret existingCaCertSecret,
                                 Secret clusterOperatorSecret,
                                 CertManagerCertificateOperator certificateOperator,
                                 SecretOperator secretOperator) {
        super(reconciliation, caRole, caConfig, kafkaCr, existingCaCertSecret, null);
        this.certificateAuthority = switch (caRole) {
            case CLUSTER_CA -> kafkaCr.getSpec().getClusterCa();
            case CLIENTS_CA -> kafkaCr.getSpec().getClientsCa();
        };
        this.clusterOperatorSecret = clusterOperatorSecret;
        this.certificateOperator = certificateOperator;
        this.secretOperator = secretOperator;
    }

    @Override
    public CompletionStage<CaProviderResult> createAndReconcileCa() {
        if (certificateAuthority.getCertManager() == null) {
            return CompletableFuture.failedFuture(new InvalidResourceException("When CA type is set to cert-manager.io, certManager property is required (e.g. clusterCa.certManager)."));
        }
        return getCaCertForCertManager()
                .thenCompose(newCaCertAsBase64 -> {
                    CertManagerCa certManagerCa = new CertManagerCa(reconciliation, caRole,
                            existingCaCertSecret,
                            caConfig,
                            certificateOperator,
                            secretOperator,
                            caConfig.isGenerateSecretOwnerRef() ? new OwnerReferenceBuilder()
                                    .withApiVersion(kafkaCr.getApiVersion())
                                    .withKind(kafkaCr.getKind())
                                    .withName(kafkaCr.getMetadata().getName())
                                    .withUid(kafkaCr.getMetadata().getUid())
                                    .withBlockOwnerDeletion(true)
                                    .withController(false)
                                    .build()
                                    : null,
                            certificateAuthority.getCertManager().getIssuerRef());
                    certManagerCa.maybeUpdateCa(
                            newCaCertAsBase64,
                            existingCaCertSecret == null ? null : Annotations.stringAnnotation(existingCaCertSecret, Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, ""),
                            CertificateUtils.cert(clusterOperatorSecret, "cluster-operator.crt")
                    );
                    Secret caCertSecret = createCaCertSecret(caRole, certManagerCa.caCertData(),
                            certManagerCa.caCertGeneration(), certManagerCa.caKeyGeneration());
                    return secretOperator.reconcile(reconciliation, reconciliation.namespace(), caCertSecret.getMetadata().getName(), caCertSecret)
                            .thenApply(i -> new CaProviderResult(certManagerCa, caCertSecret));
                });
    }

    private CompletionStage<String> getCaCertForCertManager() {
        String caCertSecretName = certificateAuthority.getCertManager().getCaCert().getSecretName();
        String caCertSecretKey = certificateAuthority.getCertManager().getCaCert().getCertificate();
        return secretOperator.getAsync(reconciliation.namespace(), caCertSecretName)
                .thenApply(secret -> {
                    if (secret == null) {
                        throw new InvalidResourceException("CA public certificate Secret " + caCertSecretName + " missing.");
                    } else if (secret.getData().get(caCertSecretKey) == null) {
                        throw new InvalidResourceException("CA public certificate Secret " + caCertSecretName + " missing key " + caCertSecretKey);
                    }
                    CertificateUtils.validateUserCaCertChain(reconciliation, caRole, Map.of(caCertSecretKey, secret.getData().get(caCertSecretKey)));
                    return secret.getData().get(caCertSecretKey);
                });
    }

    private Secret createCaCertSecret(Ca.CaRole caRole, Map<String, String> caCertData, int caCertGeneration, int caKeyGeneration) {
        Map<String, String> certAnnotations = new HashMap<>(2);

        try {
            certAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(caCertData.get(CA_CRT)))));
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
        String secretName = switch (caRole) {
            case CLUSTER_CA -> {
                certAnnotations.put(ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(caKeyGeneration));
                yield AbstractModel.clusterCaCertSecretName(reconciliation.name());
            }
            case CLIENTS_CA -> KafkaResources.clientsCaCertificateSecretName(reconciliation.name());
        };

        return createCaCertSecret(caRole, secretName, caCertData, certAnnotations, caCertGeneration);
    }
}
