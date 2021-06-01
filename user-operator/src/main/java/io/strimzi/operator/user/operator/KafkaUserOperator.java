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
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
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
    private final String caCertName;
    private final String caKeyName;
    private final String caNamespace;
    private final ScramShaCredentialsOperator scramShaCredentialOperator;
    private final KafkaUserQuotasOperator kafkaUserQuotasOperator;
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(12);
    private final String secretPrefix;

    /**
     * @param vertx The Vertx instance.
     * @param certManager For managing certificates.
     * @param crdOperator For operating on Custom Resources.
     * @param labels A selector for which users in the namespace to consider as the operators
     * @param secretOperations For operating on Secrets.
     * @param scramShaCredentialOperator For operating on SCRAM SHA credentials.
     * @param kafkaUserQuotasOperator For operating on Kafka User quotas.
     * @param aclOperations For operating on ACLs.
     * @param caCertName The name of the Secret containing the clients CA certificate.
     * @param caKeyName The name of the Secret containing the clients CA private key.
     * @param caNamespace The namespace of the Secret containing the clients CA certificate and private key.
     * @param secretPrefix The prefix used to add to the name of the Secrets generated from the KafkaUser resources.
     */
    public KafkaUserOperator(Vertx vertx,
                             CertManager certManager,
                             CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> crdOperator,
                             Labels labels,
                             SecretOperator secretOperations,
                             ScramShaCredentialsOperator scramShaCredentialOperator,
                             KafkaUserQuotasOperator kafkaUserQuotasOperator,
                             SimpleAclOperator aclOperations, String caCertName, String caKeyName, String caNamespace, String secretPrefix) {
        super(vertx, "KafkaUser", crdOperator, new MicrometerMetricsProvider(), labels);
        this.certManager = certManager;
        this.secretOperations = secretOperations;
        this.scramShaCredentialOperator = scramShaCredentialOperator;
        this.kafkaUserQuotasOperator = kafkaUserQuotasOperator;
        this.aclOperations = aclOperations;
        this.caCertName = caCertName;
        this.caKeyName = caKeyName;
        this.caNamespace = caNamespace;
        this.secretPrefix = secretPrefix;
    }

    @Override
    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return CompositeFuture.join(super.allResourceNames(namespace),
                invokeAsync(aclOperations::getUsersWithAcls),
                invokeAsync(scramShaCredentialOperator::list)).map(compositeFuture -> {
                    Set<NamespaceAndName> names = compositeFuture.resultAt(0);
                    names.addAll(toResourceRef(namespace, compositeFuture.resultAt(1)));
                    names.addAll(toResourceRef(namespace, compositeFuture.resultAt(2)));
                    return names;
                });
    }

    List<NamespaceAndName> toResourceRef(String namespace, Collection<String> names) {
        return names.stream()
                .map(name -> new NamespaceAndName(namespace, name))
                .collect(Collectors.toList());
    }

    private <T> Future<T> invokeAsync(Supplier<T> getter) {
        Promise<T> result = Promise.promise();
        vertx.createSharedWorkerExecutor("zookeeper-ops-pool").executeBlocking(future -> {
            try {
                future.complete(getter.get());
            } catch (Throwable t) {
                future.fail(t);
            }
        },
            true,
            result);
        return result.future();
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
        Secret clientsCaCert = secretOperations.get(caNamespace, caCertName);
        Secret clientsCaKey = secretOperations.get(caNamespace, caKeyName);
        Secret userSecret = secretOperations.get(reconciliation.namespace(), KafkaUserModel.getSecretName(secretPrefix, reconciliation.name()));

        KafkaUserStatus userStatus = new KafkaUserStatus();
        String namespace = reconciliation.namespace();
        String userName = reconciliation.name();
        KafkaUserModel user;

        try {
            user = KafkaUserModel.fromCrd(reconciliation, certManager, passwordGenerator, resource, clientsCaCert, clientsCaKey, userSecret, secretPrefix);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(resource, userStatus, Future.failedFuture(e));
            return Future.failedFuture(new ReconciliationException(userStatus, e));
        }

        LOGGER.debugCr(reconciliation, "Updating User {} in namespace {}", userName, namespace);
        Secret desired = user.generateSecret();
        String password = null;

        if (desired != null && desired.getData().get("password") != null)   {
            password = new String(Base64.getDecoder().decode(desired.getData().get("password")), StandardCharsets.US_ASCII);
        }

        Set<SimpleAclRule> tlsAcls = null;
        Set<SimpleAclRule> scramOrNoneAcls = null;
        KafkaUserQuotas tlsQuotas = null;
        KafkaUserQuotas scramOrNoneQuotas = null;

        if (user.isTlsUser())   {
            tlsAcls = user.getSimpleAclRules();
            tlsQuotas = user.getQuotas();
        } else if (user.isScramUser() || user.isNoneUser())  {
            scramOrNoneAcls = user.getSimpleAclRules();
            scramOrNoneQuotas = user.getQuotas();
        }

        // Create the effectively final variables to use in lambda
        KafkaUserQuotas finalScramOrNoneQuotas = scramOrNoneQuotas;
        KafkaUserQuotas finalTlsQuotas = tlsQuotas;

        Promise<KafkaUserStatus> handler = Promise.promise();

        // Reconciliation of Quotas and of SCRAM-SHA credentials changes the same fields and cannot be done in parallel
        // because they would overwrite each other's data!
        CompositeFuture.join(
                scramShaCredentialOperator.reconcile(user.getName(), password)
                        .compose(ignore -> CompositeFuture.join(kafkaUserQuotasOperator.reconcile(KafkaUserModel.getTlsUserName(userName), finalTlsQuotas),
                                kafkaUserQuotasOperator.reconcile(KafkaUserModel.getScramUserName(userName), finalScramOrNoneQuotas))),
                reconcileSecretAndSetStatus(reconciliation, namespace, user, desired, userStatus),
                aclOperations.reconcile(KafkaUserModel.getTlsUserName(userName), tlsAcls),
                aclOperations.reconcile(KafkaUserModel.getScramUserName(userName), scramOrNoneAcls))
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

    protected Future<ReconcileResult<Secret>> reconcileSecretAndSetStatus(Reconciliation reconciliation, String namespace, KafkaUserModel user, Secret desired, KafkaUserStatus userStatus) {
        return secretOperations.reconcile(reconciliation, namespace, user.getSecretName(), desired).compose(ar -> {
            if (desired != null) {
                userStatus.setSecret(desired.getMetadata().getName());
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
        return CompositeFuture.join(secretOperations.reconcile(reconciliation, namespace, KafkaUserModel.getSecretName(secretPrefix, user), null),
                aclOperations.reconcile(KafkaUserModel.getTlsUserName(user), null),
                aclOperations.reconcile(KafkaUserModel.getScramUserName(user), null),
                scramShaCredentialOperator.reconcile(KafkaUserModel.getScramUserName(user), null)
                        .compose(ignore -> kafkaUserQuotasOperator.reconcile(KafkaUserModel.getTlsUserName(user), null))
                        .compose(ignore -> kafkaUserQuotasOperator.reconcile(KafkaUserModel.getScramUserName(user), null)))
            .map(Boolean.TRUE);
    }

    @Override
    protected KafkaUserStatus createStatus() {
        return new KafkaUserStatus();
    }
}
