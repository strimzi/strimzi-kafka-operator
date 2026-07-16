/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.InternalCa;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.strimzi.operator.common.ca.Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION;

/**
 * CA provider for Strimzi-managed CAs.
 * Handles creation and reconciliation of self-signed CA certificates.
 */
public class InternalCaProvider extends CaProvider {
    private final SecretOperator secretOperator;
    private final CertIssuer certIssuer;
    private final PasswordGenerator passwordGenerator;
    private final Clock clock;

    /**
     * Constructor.
     *
     * @param reconciliation        Reconciliation marker
     * @param caRole                The role of this CA
     * @param caConfig              CA configuration
     * @param kafkaCr               The Kafka custom resource
     * @param secretOperator        Secret operator for managing secrets
     * @param certIssuer            Certificate issuer
     * @param passwordGenerator     Password generator
     * @param clock                 Clock for time-based operations
     * @param existingCaCertSecret  Existing CA certificate secret
     * @param existingCaKeySecret   Existing CA key secret
     */
    InternalCaProvider(Reconciliation reconciliation, Ca.CaRole caRole, CaConfig caConfig, Kafka kafkaCr, SecretOperator secretOperator, CertIssuer certIssuer, PasswordGenerator passwordGenerator, Clock clock, Secret existingCaCertSecret, Secret existingCaKeySecret) {
        super(reconciliation, caRole, caConfig, kafkaCr, existingCaCertSecret, existingCaKeySecret);
        this.secretOperator = secretOperator;
        this.certIssuer = certIssuer;
        this.passwordGenerator = passwordGenerator;
        this.clock = clock;
    }

    @Override
    public CompletionStage<CaProviderResult> createAndReconcileCa() {
        InternalCa internalCa = new InternalCa(reconciliation, caRole, certIssuer, passwordGenerator, existingCaCertSecret, existingCaKeySecret, caConfig);
        internalCa.createRenewOrReplace(Util.isMaintenanceTimeWindowsSatisfied(reconciliation, kafkaCr.getSpec().getMaintenanceTimeWindows(), clock.instant()),
                isForceReplace(existingCaKeySecret),
                isForceRenew(existingCaCertSecret));

        Secret caKeySecret = createCaKeySecret(internalCa);
        Secret caCertSecret = createCaCertSecret(internalCa);

        CompletableFuture<ReconcileResult<Secret>> caCertSecretFuture = secretOperator.reconcile(reconciliation, reconciliation.namespace(),
                caCertSecret.getMetadata().getName(), caCertSecret).toCompletableFuture();
        CompletableFuture<ReconcileResult<Secret>> caKeySecretFuture = secretOperator.reconcile(reconciliation, reconciliation.namespace(),
                caKeySecret.getMetadata().getName(), caKeySecret).toCompletableFuture();
        return CompletableFuture.allOf(caCertSecretFuture, caKeySecretFuture)
                .thenApply(v -> new CaProviderResult(internalCa, caCertSecret));
    }

    private boolean isForceReplace(Secret caSecret) {
        if (caSecret != null && caSecret.getMetadata() != null &&
                Annotations.hasAnnotation(caSecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE)) {
            return Annotations.booleanAnnotation(caSecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, false);
        } else {
            return false;
        }
    }

    private boolean isForceRenew(Secret caSecret) {
        if (caSecret != null && caSecret.getMetadata() != null &&
                Annotations.hasAnnotation(caSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW)) {
            return Annotations.booleanAnnotation(caSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, false);
        } else {
            return false;
        }
    }

    private Secret createCaCertSecret(InternalCa internalCa) {
        Map<String, String> certAnnotations = new HashMap<>(1);

        if (internalCa.postponed() && Annotations.hasAnnotation(existingCaCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW))   {
            certAnnotations.put(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, Annotations.stringAnnotation(existingCaCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "false"));
        }
        String caCertSecretName = switch (caRole) {
            case CLUSTER_CA -> AbstractModel.clusterCaCertSecretName(reconciliation.name());
            case CLIENTS_CA -> KafkaResources.clientsCaCertificateSecretName(reconciliation.name());
        };
        return createCaCertSecret(caRole, caCertSecretName, internalCa.caCertData(), certAnnotations, internalCa.caCertGeneration());
    }

    private Secret createCaKeySecret(InternalCa internalCa) {
        Map<String, String> keyAnnotations = new HashMap<>(2);
        keyAnnotations.put(ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(internalCa.caKeyGeneration()));

        if (internalCa.postponed()
                && existingCaKeySecret != null
                && Annotations.hasAnnotation(existingCaKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE))   {
            keyAnnotations.put(Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, Annotations.stringAnnotation(existingCaKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "false"));
        }
        String caKeySecretName = switch (caRole) {
            case CLUSTER_CA -> AbstractModel.clusterCaKeySecretName(reconciliation.name());
            case CLIENTS_CA -> KafkaResources.clientsCaKeySecretName(reconciliation.name());
        };
        return createCaSecret(caKeySecretName, internalCa.caKeyData(), caLabels, keyAnnotations);
    }
}
