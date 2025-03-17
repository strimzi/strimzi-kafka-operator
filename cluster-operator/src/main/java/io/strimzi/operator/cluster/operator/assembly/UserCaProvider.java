/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.InternalCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * CA provider for user-provided CAs.
 * Uses existing CA certificates and keys provided by the user.
 */
public class UserCaProvider extends CaProvider {
    private final CertIssuer certIssuer;
    private final PasswordGenerator passwordGenerator;

    /**
     * Constructor.
     *
     * @param reconciliation    Reconciliation marker
     * @param caRole            The role of this CA
     * @param caConfig          CA configuration
     * @param kafkaCr           The Kafka custom resource
     * @param certIssuer        Certificate issuer
     * @param passwordGenerator Password generator
     * @param existingCaCert    Existing CA certificate secret
     * @param existingCaKey     Existing CA key secret
     */
    public UserCaProvider(Reconciliation reconciliation, Ca.CaRole caRole, CaConfig caConfig, Kafka kafkaCr, CertIssuer certIssuer, PasswordGenerator passwordGenerator, Secret existingCaCert, Secret existingCaKey) {
        super(reconciliation, caRole, caConfig, kafkaCr, existingCaCert, existingCaKey);
        this.certIssuer = certIssuer;
        this.passwordGenerator = passwordGenerator;
    }

    @Override
    public CompletionStage<Ca> createCa() {
        if (existingCaCertSecret == null || existingCaKeySecret == null)   {
            throw new InvalidResourceException(caRole.caName() + " should not be generated, but the secrets were not found.");
        }
        validateUserCaCertChain(existingCaCertSecret.getData());
        return CompletableFuture.completedStage(new InternalCa(reconciliation, caRole, certIssuer, passwordGenerator, existingCaCertSecret, existingCaKeySecret, caConfig));
    }

    @Override
    public CompletionStage<Secret> reconcileCaSecrets() {
        return CompletableFuture.completedStage(existingCaCertSecret);
    }
}
