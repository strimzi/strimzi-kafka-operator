/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
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
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Operator for a Kafka Users.
 */
public class KafkaUserOperator extends AbstractOperator<KafkaUser,
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser>> {
    private static final Logger log = LogManager.getLogger(KafkaUserOperator.class.getName());

    private final SecretOperator secretOperations;
    private final SimpleAclOperator aclOperations;
    private final CertManager certManager;
    private final String caCertName;
    private final String caKeyName;
    private final String caNamespace;
    private final ScramShaCredentialsOperator scramShaCredentialOperator;
    private final Optional<LabelSelector> selector;
    private PasswordGenerator passwordGenerator = new PasswordGenerator(12);

    /**
     * @param vertx The Vertx instance.
     * @param certManager For managing certificates.
     * @param crdOperator For operating on Custom Resources.
     * @param labels A selector for which users in the namespace to consider as the operators
     * @param secretOperations For operating on Secrets.
     * @param scramShaCredentialOperator For operating on SCRAM SHA credentials.
     * @param aclOperations For operating on ACLs.
     * @param caCertName The name of the Secret containing the clients CA certificate.
     * @param caKeyName The name of the Secret containing the clients CA private key.
     * @param caNamespace The namespace of the Secret containing the clients CA certificate and private key.
     */
    public KafkaUserOperator(Vertx vertx,
                             CertManager certManager,
                             CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> crdOperator,
                             Labels labels,
                             SecretOperator secretOperations,
                             ScramShaCredentialsOperator scramShaCredentialOperator,
                             SimpleAclOperator aclOperations, String caCertName, String caKeyName, String caNamespace) {
        super(vertx, "User", crdOperator);
        this.certManager = certManager;
        Map<String, String> matchLabels = labels.toMap();
        this.selector = matchLabels.isEmpty() ? Optional.empty() : Optional.of(new LabelSelector(null, matchLabels));
        this.secretOperations = secretOperations;
        this.scramShaCredentialOperator = scramShaCredentialOperator;
        this.aclOperations = aclOperations;
        this.caCertName = caCertName;
        this.caKeyName = caKeyName;
        this.caNamespace = caNamespace;
    }

    @Override
    public Optional<LabelSelector> selector() {
        return selector;
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
        Future<T> result = Future.future();
        vertx.createSharedWorkerExecutor("zookeeper-ops-pool").executeBlocking(future -> {
            try {
                future.complete(getter.get());
            } catch (Throwable t) {
                future.fail(t);
            }
        },
            true,
            result);
        return result;
    }

    /**
     * Creates or updates the user. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param reconciliation Unique identification for the reconciliation
     * @param resource KafkaUser resources with the desired user configuration.
     * @return a Future
     */
    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaUser resource) {
        Future<Void> handler = Future.future();
        Secret clientsCaCert = secretOperations.get(caNamespace, caCertName);
        Secret clientsCaKey = secretOperations.get(caNamespace, caKeyName);
        Secret userSecret = secretOperations.get(reconciliation.namespace(), KafkaUserModel.getSecretName(reconciliation.name()));

        Future<Void> createOrUpdateFuture = Future.future();
        String namespace = reconciliation.namespace();
        String userName = reconciliation.name();
        KafkaUserModel user;
        KafkaUserStatus userStatus = new KafkaUserStatus();
        try {
            user = KafkaUserModel.fromCrd(certManager, passwordGenerator, resource, clientsCaCert, clientsCaKey, userSecret);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(resource, userStatus, Future.failedFuture(e));
            updateStatus(resource, reconciliation, userStatus)
                    .setHandler(result -> handler.handle(Future.failedFuture(e)));
            return handler;
        }

        log.debug("{}: Updating User {} in namespace {}", reconciliation, userName, namespace);
        Secret desired = user.generateSecret();
        String password = null;

        if (desired != null && desired.getData().get("password") != null)   {
            password = new String(Base64.getDecoder().decode(desired.getData().get("password")), StandardCharsets.US_ASCII);
        }

        Set<SimpleAclRule> tlsAcls = null;
        Set<SimpleAclRule> scramOrNoneAcls = null;

        if (user.isTlsUser())   {
            tlsAcls = user.getSimpleAclRules();
        } else if (user.isScramUser() || user.isNoneUser())  {
            scramOrNoneAcls = user.getSimpleAclRules();
        }

        CompositeFuture.join(
                scramShaCredentialOperator.reconcile(user.getName(), password),
                reconcileSecretAndSetStatus(namespace, user, desired, userStatus),
                aclOperations.reconcile(KafkaUserModel.getTlsUserName(userName), tlsAcls),
                aclOperations.reconcile(KafkaUserModel.getScramUserName(userName), scramOrNoneAcls))
                .setHandler(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(resource, userStatus, reconciliationResult.mapEmpty());
                    userStatus.setUsername(user.getUserName());

                    updateStatus(resource, reconciliation, userStatus).setHandler(statusResult -> {
                        // If both features succeeded, createOrUpdate succeeded as well
                        // If one or both of them failed, we prefer the reconciliation failure as the main error
                        if (reconciliationResult.succeeded() && statusResult.succeeded()) {
                            createOrUpdateFuture.complete();
                        } else if (reconciliationResult.failed()) {
                            createOrUpdateFuture.fail(reconciliationResult.cause());
                        } else {
                            createOrUpdateFuture.fail(statusResult.cause());
                        }
                        handler.handle(createOrUpdateFuture);
                    });
                });
        return handler;
    }

    protected Future<ReconcileResult<Secret>> reconcileSecretAndSetStatus(String namespace, KafkaUserModel user, Secret desired, KafkaUserStatus userStatus) {
        return secretOperations.reconcile(namespace, user.getSecretName(), desired).compose(ar -> {
            if (desired != null) {
                userStatus.setSecret(desired.getMetadata().getName());
            }
            return Future.succeededFuture(ar);
        });
    }

    /**
     * Updates the Status field of the Kafka User CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param kafkaUserAssembly The CR of Kafka user
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaUserStatus which should be set
     *
     * @return
     */
    Future<Void> updateStatus(KafkaUser kafkaUserAssembly, Reconciliation reconciliation, KafkaUserStatus desiredStatus) {
        Future<Void> updateStatusFuture = Future.future();

        resourceOperator.getAsync(kafkaUserAssembly.getMetadata().getNamespace(), kafkaUserAssembly.getMetadata().getName()).setHandler(getRes -> {
            if (getRes.succeeded()) {
                KafkaUser user = getRes.result();

                if (user != null) {
                    if (StatusUtils.isResourceV1alpha1(user)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, user.getApiVersion());
                        updateStatusFuture.complete();
                    } else {
                        KafkaUserStatus currentStatus = user.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            KafkaUser resourceWithNewStatus = new KafkaUserBuilder(user).withStatus(desiredStatus).build();

                            resourceOperator.updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
                                if (updateRes.succeeded()) {
                                    log.debug("{}: Completed status update", reconciliation);
                                    updateStatusFuture.complete();
                                } else {
                                    log.error("{}: Failed to update status", reconciliation, updateRes.cause());
                                    updateStatusFuture.fail(updateRes.cause());
                                }
                            });
                        } else {
                            log.debug("{}: Status did not change", reconciliation);
                            updateStatusFuture.complete();
                        }
                    }
                } else {
                    log.error("{}: Current Kafka resource not found", reconciliation);
                    updateStatusFuture.fail("Current Kafka User resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka User resource and its status", reconciliation, getRes.cause());
                updateStatusFuture.fail(getRes.cause());
            }
        });

        return updateStatusFuture;
    }

    /**
     * Deletes the user
     *
     * @reutrn A Future
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String user = reconciliation.name();
        log.debug("{}: Deleting User", reconciliation, user, namespace);
        return CompositeFuture.join(secretOperations.reconcile(namespace, KafkaUserModel.getSecretName(user), null),
                aclOperations.reconcile(KafkaUserModel.getTlsUserName(user), null),
                aclOperations.reconcile(KafkaUserModel.getScramUserName(user), null),
                scramShaCredentialOperator.reconcile(KafkaUserModel.getScramUserName(user), null))
            .map(Boolean.TRUE);
    }

}
