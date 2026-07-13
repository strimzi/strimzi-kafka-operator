/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.model.Labels;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static io.strimzi.operator.common.ca.Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Abstract base class for Certificate Authority (CA) providers.
 * Provides common functionality for managing CA certificates and secrets.
 */
public abstract class CaProvider {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CaProvider.class);

    protected final Reconciliation reconciliation;
    protected final Ca.CaRole caRole;
    protected final CaConfig caConfig;
    protected final Kafka kafkaCr;
    protected final Map<String, String> caLabels;
    protected final Secret existingCaCertSecret;
    protected final Secret existingCaKeySecret;
    protected Ca ca;

    /**
     * Constructor.
     *
     * @param reconciliation        Reconciliation marker
     * @param caRole                The role of this CA
     * @param caConfig              CA configuration
     * @param kafkaCr               The Kafka custom resource
     * @param existingCaCertSecret  Existing CA certificate secret
     * @param existingCaKeySecret   Existing CA key secret
     */
    public CaProvider(Reconciliation reconciliation, Ca.CaRole caRole, CaConfig caConfig, Kafka kafkaCr, Secret existingCaCertSecret, Secret existingCaKeySecret) {
        this.reconciliation = reconciliation;
        this.caRole = caRole;
        this.caConfig = caConfig;
        this.kafkaCr = kafkaCr;
        this.caLabels = Labels.generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, "certificate-authority", AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME).toMap();
        this.existingCaCertSecret = existingCaCertSecret;
        this.existingCaKeySecret = existingCaKeySecret;
    }

    /**
     * Creates or loads the CA instance.
     *
     * @return CompletionStage that completes with the CA instance
     */
    public abstract CompletionStage<Ca> createCa();

    /**
     * Reconciles the CA secrets with the Kubernetes cluster.
     *
     * @return CompletionStage that completes with the reconciled CA certificate secret
     */
    public abstract CompletionStage<Secret> reconcileCaSecrets();

    /**
     * Method to extract the template labels from the Kafka CR.
     *
     * @return  Map with the labels from the Kafka CR or empty map if the template is not set
     */
    private Map<String, String> clusterCaCertLabels() {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getTemplate() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels() != null) {
            return kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels();
        } else {
            return Map.of();
        }
    }

    /**
     * Method to extract the template annotations from the Kafka CR.
     *
     * @return  Map with the annotation from the Kafka CR or empty map if the template is not set
     */
    private Map<String, String> clusterCaCertAnnotations() {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getTemplate() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations() != null) {
            return kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations();
        } else {
            return Map.of();
        }
    }

    /**
     * Validates whether each of the user provided CA certs has a valid chain. Any intermediate CAs should be provided first,
     * in order, with the root CA being the last certificate.
     *
     * @param userCaCertData The CA cert data provided by the user.
     */
    protected void validateUserCaCertChain(Map<String, String> userCaCertData) {
        userCaCertData.entrySet()
                .stream()
                .filter(entry -> Ca.SecretEntry.CRT.matchesType(entry.getKey()))
                .forEach(entry -> {
                    List<X509Certificate> certChain = CertificateUtils.extractCertChain(entry.getKey(), Util.decodeBytesFromBase64(entry.getValue()));
                    if (certChain.isEmpty()) {
                        LOGGER.errorCr(reconciliation, "{} certificate chain in {} is empty", caRole.caName(), entry.getKey());
                        throw new RuntimeException("Failed to validate User supplied " + caRole.caName() + " cert chain in " + entry.getKey());
                    } else if (certChain.size() == 1) {
                        LOGGER.debugCr(reconciliation, "{} certificate {} contains a single certificate", caRole.caName(), entry.getKey());
                        return;
                    }
                    if (!CertificateUtils.certIsTrusted(reconciliation, certChain.subList(0, certChain.size() - 1), certChain.getLast())) {
                        String errorMessage = "User supplied " + caRole.caName() + " cert chain " + entry.getKey() + " is not valid. Certificates must be provided in the correct order.";
                        LOGGER.errorCr(reconciliation, errorMessage);
                        throw new RuntimeException(errorMessage);
                    }
                });
    }

    protected Secret createCaCertSecret(Ca.CaRole caRole, String name, Map<String, String> data, Map<String, String> certAnnotations, int caCertGeneration) {
        Map<String, String> annotations = new HashMap<>();
        annotations.put(ANNO_STRIMZI_IO_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
        annotations.putAll(certAnnotations);

        return switch (caRole) {
            case CLUSTER_CA ->
                    createCaSecret(name, data, Util.mergeLabelsOrAnnotations(caLabels, clusterCaCertLabels()),
                            Util.mergeLabelsOrAnnotations(annotations, clusterCaCertAnnotations()));
            case CLIENTS_CA -> createCaSecret(name, data, caLabels, annotations);
        };
    }

    protected Secret createCaSecret(String name, Map<String, String> data, Map<String, String> labels, Map<String, String> annotations) {
        List<OwnerReference>  ownerReferences = caConfig.isGenerateSecretOwnerRef() ?
                singletonList(new OwnerReferenceBuilder()
                        .withApiVersion(kafkaCr.getApiVersion())
                        .withKind(kafkaCr.getKind())
                        .withName(kafkaCr.getMetadata().getName())
                        .withUid(kafkaCr.getMetadata().getUid())
                        .withBlockOwnerDeletion(true)
                        .withController(false)
                        .build()) :
                emptyList();
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(reconciliation.namespace())
                    .withLabels(labels)
                    .withAnnotations(annotations)
                    .withOwnerReferences(ownerReferences)
                .endMetadata()
                .withType("Opaque")
                .withData(data)
                .build();
    }
}
