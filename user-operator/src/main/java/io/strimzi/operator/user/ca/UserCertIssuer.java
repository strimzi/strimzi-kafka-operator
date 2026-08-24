/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.ca;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.certmanager.IssuerRef;
import io.strimzi.api.kafka.model.kafka.certmanager.IssuerRefBuilder;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.user.UserOperatorConfig;

import java.time.Clock;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * Certificate issuer for issuing user certificates using the Clients CA.
 */
public interface UserCertIssuer {
    /**
     * Maybe copy or generate user certificate using Clients CA
     *
     * @param reconciliation       Reconciliation marker
     * @param caCertSecret         Secret containing the CA certificate
     * @param caKeySecret          Secret containing the CA private key
     * @param userSecret           Existing user secret, if it exists
     * @param userName             Name of the user
     * @param validityDays         Certificate validity in days
     * @param renewalDays          Certificate renewal period in days
     * @param generatePkcs12Stores Whether to generate PKCS12 keystores
     * @param ownerReference       Owner reference to add to any created resources
     * @return CompletionStage with the CA cert and generated user certificate
     */
    CompletionStage<UserCertResult> maybeCopyOrGenerateCert(
            Reconciliation reconciliation,
            Secret caCertSecret,
            Secret caKeySecret,
            Secret userSecret,
            String userName,
            int validityDays,
            int renewalDays,
            boolean generatePkcs12Stores,
            OwnerReference ownerReference);

    /**
     * Creates the appropriate UserCertIssuer based on the configured certificate manager type
     *
     * @param config            User Operator configuration
     * @param executor          Executor service for async operations
     * @param client            Kubernetes client
     * @param secretOperator    Secret operator for managing secrets
     *
     * @return  The configured UserCertIssuer instance
     */
    static UserCertIssuer createUserCertIssuer(UserOperatorConfig config, ExecutorService executor,
                                               KubernetesClient client, SecretOperator secretOperator) {
        return switch (config.getCertificateManagerType()) {
            case STRIMZI -> new InternalCaUserCertIssuer(
                    new OpenSslCertIssuer(),
                    new PasswordGenerator(config.getScramPasswordLength()),
                    config.getMaintenanceWindows(),
                    Clock.systemUTC());
            case CERT_MANAGER -> {
                CertManagerCertificateOperator certManagerOp = new CertManagerCertificateOperator(executor, client);
                IssuerRef issuerRef = new IssuerRefBuilder()
                        .withName(config.getCertManagerIssuerName())
                        .withKind(config.getCertManagerIssuerKind())
                        .withGroup(config.getCertManagerIssuerGroup())
                        .build();
                yield new CertManagerCaUserCertIssuer(certManagerOp, secretOperator, issuerRef);
            }
        };
    }

}
