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
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.time.Clock;
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

    /**
     * Creates a CA provider.
     *
     * @param reconciliation        Reconciliation marker
     * @param caRole                The role of the CA
     * @param caConfig              CA configuration
     * @param kafkaCr               The Kafka custom resource
     * @param secretOperator        Secret operator for managing secrets
     * @param certIssuer            Certificate issuer
     * @param passwordGenerator     Password generator
     * @param clock                 Clock for time-based operations
     * @param existingCaCertSecret  Existing CA certificate secret
     * @param existingCaKeySecret   Existing CA key secret
     *
     * @return The created CaProvider instance
     */
    static CaProvider create(
            Reconciliation reconciliation,
            Ca.CaRole caRole,
            CaConfig caConfig,
            Kafka kafkaCr,
            SecretOperator secretOperator,
            CertIssuer certIssuer,
            PasswordGenerator passwordGenerator,
            Clock clock,
            Secret existingCaCertSecret,
            Secret existingCaKeySecret
    ) {
        if (caConfig.isGenerateCa()) {
            return new InternalCaProvider(reconciliation, caRole, caConfig, kafkaCr, secretOperator, certIssuer,
                    passwordGenerator, clock, existingCaCertSecret, existingCaKeySecret
            );
        } else {
            return new CustomCaProvider(reconciliation, caRole, caConfig, kafkaCr, certIssuer, passwordGenerator,
                    existingCaCertSecret, existingCaKeySecret
            );
        }
    }

    /**
     * Constructor.
     *
     * @param reconciliation        Reconciliation marker
     * @param caRole                The role of the CA
     * @param caConfig              CA configuration
     * @param kafkaCr               The Kafka custom resource
     * @param existingCaCertSecret  Existing CA certificate secret
     * @param existingCaKeySecret   Existing CA key secret
     */
    CaProvider(Reconciliation reconciliation, Ca.CaRole caRole, CaConfig caConfig, Kafka kafkaCr, Secret existingCaCertSecret, Secret existingCaKeySecret) {
        this.reconciliation = reconciliation;
        this.caRole = caRole;
        this.caConfig = caConfig;
        this.kafkaCr = kafkaCr;
        this.caLabels = Labels.generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, "certificate-authority", AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME).toMap();
        this.existingCaCertSecret = existingCaCertSecret;
        this.existingCaKeySecret = existingCaKeySecret;
    }

    /**
     * Creates or loads the CA instance and reconciles the CA secrets with the Kubernetes cluster.
     *
     * @return CompletionStage that completes with the CA instance and reconciled CA certificate secret
     */
    public abstract CompletionStage<CaProviderResult> createAndReconcileCa();

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
