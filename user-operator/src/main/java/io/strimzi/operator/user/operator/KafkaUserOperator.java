/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.KafkaUserSpec;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Operator for a Kafka Users.
 */
public class KafkaUserOperator extends AbstractOperator<KafkaUser, KafkaUserSpec, KafkaUserStatus,
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaUserOperator.class.getName());

    private final SecretOperator secretOperations;
    private final SimpleAclOperator aclOperations;
    private final CertManager certManager;
    private final ScramCredentialsOperator scramCredentialsOperator;
    private final QuotasOperator quotasOperator;
    private final UserOperatorConfig config;
    private final PasswordGenerator passwordGenerator;

    /**
     * Creates the instance of KafkaUserOperator
     *
     * @param vertx The Vertx instance.
     * @param certManager For managing certificates.
     * @param crdOperator For operating on Custom Resources.
     * @param secretOperations For operating on Secrets.
     * @param scramCredentialsOperator For operating on SCRAM SHA credentials.
     * @param quotasOperator For operating on Kafka User quotas.
     * @param aclOperations For operating on ACLs.
     * @param config User operator configuration
     */
    public KafkaUserOperator(Vertx vertx,
                             CertManager certManager,
                             CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> crdOperator,
                             SecretOperator secretOperations,
                             ScramCredentialsOperator scramCredentialsOperator,
                             QuotasOperator quotasOperator,
                             SimpleAclOperator aclOperations,
                             UserOperatorConfig config) {
        super(vertx, "KafkaUser", crdOperator, new MicrometerMetricsProvider(), config.getLabels());
        this.certManager = certManager;
        this.secretOperations = secretOperations;
        this.scramCredentialsOperator = scramCredentialsOperator;
        this.quotasOperator = quotasOperator;
        this.aclOperations = aclOperations;
        this.config = config;
        this.passwordGenerator = new PasswordGenerator(this.config.getScramPasswordLength());
    }

    @Override
    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return CompositeFuture.join(super.allResourceNames(namespace),
                config.isAclsAdminApiSupported() ? aclOperations.getAllUsers() : Future.succeededFuture(Set.of()),
                quotasOperator.getAllUsers(),
                scramCredentialsOperator.getAllUsers()).map(compositeFuture -> {
                    Set<NamespaceAndName> names = compositeFuture.resultAt(0);
                    names.addAll(toResourceRef(namespace, compositeFuture.resultAt(1)));
                    names.addAll(toResourceRef(namespace, compositeFuture.resultAt(2)));
                    names.addAll(toResourceRef(namespace, compositeFuture.resultAt(3)));
                    return names;
                });
    }

    List<NamespaceAndName> toResourceRef(String namespace, Collection<String> names) {
        return names.stream()
                .map(name -> new NamespaceAndName(namespace, name))
                .collect(Collectors.toList());
    }

    /**
     * Creates or updates the user. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     *
     * @param reconciliation Unique identification for the reconciliation
     * @param resource KafkaUser resources with the desired user configuration.
     * @return a Future
     */
    @Override
    protected Future<KafkaUserStatus> createOrUpdate(Reconciliation reconciliation, KafkaUser resource) {
        KafkaUserModel user;
        KafkaUserStatus userStatus = new KafkaUserStatus();

        try {
            user = KafkaUserModel.fromCrd(resource, config.getSecretPrefix(), config.isAclsAdminApiSupported());
            LOGGER.debugCr(reconciliation, "Updating User {} in namespace {}", reconciliation.name(), reconciliation.namespace());
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(resource, userStatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(userStatus, e));
        }

        Promise<KafkaUserStatus> handler = Promise.promise();

        secretOperations.getAsync(reconciliation.namespace(), user.getSecretName())
                .compose(userSecret -> maybeGenerateCredentials(reconciliation, user, userSecret))
                .compose(ignore -> reconcileCredentialsQuotasAndAcls(reconciliation, user, userStatus))
                .onComplete(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(resource, userStatus, reconciliationResult.mapEmpty());
                    userStatus.setUsername(user.getUserName());

                    if (reconciliationResult.succeeded())   {
                        handler.complete(userStatus);
                    } else {
                        handler.fail(new ReconciliationException(userStatus, reconciliationResult.cause()));
                    }
                });

        return handler.future();
    }

    /**
     * Depending on the KafkaUser configuration and the user secret, this method will set or generate the credentials
     * for given user.
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Secret with existing user credentials or null if the secret doesn't exist yet
     *
     * @return                  Future describing the result
     */
    private Future<Void> maybeGenerateCredentials(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret)   {
        // Generates the password or user certificate
        if (user.isScramUser()) {
            return maybeGenerateScramCredentials(reconciliation, user, userSecret);
        } else if (user.isTlsUser())    {
            return maybeGenerateTlsCredentials(reconciliation, user, userSecret);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Sets or generates the credentials for a SCRAM-SHA-512 user
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Secret with existing user credentials or null if the secret doesn't exist yet
     *
     * @return                  Future describing the result
     */
    private Future<Void> maybeGenerateScramCredentials(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret)   {
        if (user.isUserWithDesiredPassword())   {
            // User is a SCRAM-SHA-512 user and requested some specific password
            return secretOperations.getAsync(reconciliation.namespace(), user.desiredPasswordSecretName())
                    .compose(desiredPasswordSecret -> {
                        user.maybeGeneratePassword(
                                reconciliation,
                                passwordGenerator,
                                userSecret,
                                desiredPasswordSecret
                        );

                        return Future.succeededFuture();
                    });
        } else {
            // User is a SCRAM-SHA-512 user and the password should be generated
            user.maybeGeneratePassword(reconciliation, passwordGenerator, userSecret, null);
            return Future.succeededFuture();
        }
    }

    /**
     * Sets or generates the credentials for a TLS user
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Secret with existing user credentials or null if the secret doesn't exist yet
     *
     * @return                  Future describing the result
     */
    private Future<Void> maybeGenerateTlsCredentials(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret)   {
        Future<Secret> caCertFuture = secretOperations.getAsync(config.getCaNamespace(), config.getCaCertSecretName());
        Future<Secret> caKeyFuture = secretOperations.getAsync(config.getCaNamespace(), config.getCaKeySecretName());

        return CompositeFuture.join(caCertFuture, caKeyFuture)
                .compose(caSecrets -> {
                    Secret clientsCaCertSecret = caSecrets.resultAt(0);
                    Secret clientsCaKeySecret = caSecrets.resultAt(1);

                    user.maybeGenerateCertificates(
                            reconciliation,
                            certManager,
                            passwordGenerator,
                            clientsCaCertSecret,
                            clientsCaKeySecret,
                            userSecret,
                            config.getClientsCaValidityDays(),
                            config.getClientsCaRenewalDays(),
                            config.getMaintenanceWindows(),
                            KafkaUserOperator::dateSupplier
                    );

                    return Future.succeededFuture();
                });
    }

    /**
     * Reconciles the credentials, quotas and ACLs
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userStatus        Status subresource of the KafkaUser custom resource
     *
     * @return                  Future describing the result
     */
    private CompositeFuture reconcileCredentialsQuotasAndAcls(Reconciliation reconciliation, KafkaUserModel user, KafkaUserStatus userStatus)   {
        Set<SimpleAclRule> tlsAcls = null;
        Set<SimpleAclRule> scramOrNoneAcls = null;
        KafkaUserQuotas tlsQuotas = null;
        KafkaUserQuotas scramOrNoneQuotas = null;

        if (user.isTlsUser() || user.isTlsExternalUser())   {
            tlsAcls = user.getSimpleAclRules();
            tlsQuotas = user.getQuotas();
        } else if (user.isScramUser() || user.isNoneUser())  {
            scramOrNoneAcls = user.getSimpleAclRules();
            scramOrNoneQuotas = user.getQuotas();
        }

        // Reconcile the user SCRAM-SHA-512 credentials
        Future<ReconcileResult<String>> scramCredentialsFuture = scramCredentialsOperator.reconcile(reconciliation, user.getName(), user.getScramSha512Password());

        // Quotas need to reconciled for both regular and TLS username. It will be (possibly) set for one user and deleted for the other
        Future<ReconcileResult<KafkaUserQuotas>> tlsQuotasFuture = quotasOperator.reconcile(reconciliation, KafkaUserModel.getTlsUserName(reconciliation.name()), tlsQuotas);
        Future<ReconcileResult<KafkaUserQuotas>> quotasFuture = quotasOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(reconciliation.name()), scramOrNoneQuotas);

        // Reconcile the user secret generated by the user operator with the credentials
        Future<ReconcileResult<Secret>> userSecretFuture = reconcileUserSecret(reconciliation, user, userStatus);

        // ACLs need to reconciled for both regular and TLS username. It will be (possibly) set for one user and deleted for the other
        Future<ReconcileResult<Set<SimpleAclRule>>> aclsTlsUserFuture;
        Future<ReconcileResult<Set<SimpleAclRule>>> aclsScramUserFuture;

        if (config.isAclsAdminApiSupported()) {
            aclsTlsUserFuture = aclOperations.reconcile(reconciliation, KafkaUserModel.getTlsUserName(reconciliation.name()), tlsAcls);
            aclsScramUserFuture = aclOperations.reconcile(reconciliation, KafkaUserModel.getScramUserName(reconciliation.name()), scramOrNoneAcls);
        } else {
            aclsTlsUserFuture = Future.succeededFuture(ReconcileResult.noop(null));
            aclsScramUserFuture = Future.succeededFuture(ReconcileResult.noop(null));
        }

        return CompositeFuture.join(scramCredentialsFuture, tlsQuotasFuture, quotasFuture, aclsTlsUserFuture, aclsScramUserFuture, userSecretFuture);
    }

    /**
     * Reconciles the Kubernetes secret with the generated credentials and sets the secret name in the KafkaUser status subresource
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userStatus        Status subresource of the KafkaUser custom resource
     *
     * @return                  Future describing the result
     */
    private Future<ReconcileResult<Secret>> reconcileUserSecret(Reconciliation reconciliation, KafkaUserModel user, KafkaUserStatus userStatus) {
        Secret desiredSecret = user.generateSecret();

        return secretOperations.reconcile(reconciliation, reconciliation.namespace(), user.getSecretName(), desiredSecret).compose(ar -> {
            if (desiredSecret != null) {
                userStatus.setSecret(desiredSecret.getMetadata().getName());
            }
            return Future.succeededFuture(ar);
        });
    }

    /**
     * Deletes the user
     *
     * @return A Future
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String user = reconciliation.name();
        LOGGER.debugCr(reconciliation, "Deleting User {} from namespace {}", user, namespace);
        return CompositeFuture.join(secretOperations.reconcile(reconciliation, namespace, KafkaUserModel.getSecretName(config.getSecretPrefix(), user), null),
                config.isAclsAdminApiSupported() ? aclOperations.reconcile(reconciliation, KafkaUserModel.getTlsUserName(user), null) : Future.succeededFuture(ReconcileResult.noop(null)),
                config.isAclsAdminApiSupported() ? aclOperations.reconcile(reconciliation, KafkaUserModel.getScramUserName(user), null) : Future.succeededFuture(ReconcileResult.noop(null)),
                scramCredentialsOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(user), null)
                        .compose(ignore -> quotasOperator.reconcile(reconciliation, KafkaUserModel.getTlsUserName(user), null))
                        .compose(ignore -> quotasOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(user), null)))
            .map(Boolean.TRUE);
    }

    @Override
    protected KafkaUserStatus createStatus() {
        return new KafkaUserStatus();
    }

    private static Date dateSupplier()  {
        return new Date();
    }
}
