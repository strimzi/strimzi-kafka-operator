/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.ResourceDiff;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;

import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Operator for a Kafka Users.
 */
public class KafkaUserOperator {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaUserOperator.class.getName());

    private final CertManager certManager;
    private final KubernetesClient client;
    private final AdminApiOperator<Set<SimpleAclRule>, Set<String>> aclOperator;
    private final AdminApiOperator<String, List<String>> scramCredentialsOperator;
    private final AdminApiOperator<KafkaUserQuotas, Set<String>> quotasOperator;
    private final ExecutorService executor;
    private final UserOperatorConfig config;
    private final PasswordGenerator passwordGenerator;
    private final LabelSelector selector;

    /**
     * Creates the instance of KafkaUserOperator
     *
     * @param config                   User operator configuration
     * @param client                   Kubernetes client
     * @param certManager              For managing certificates.
     * @param scramCredentialsOperator For operating on SCRAM SHA credentials.
     * @param quotasOperator           For operating on Kafka User quotas.
     * @param aclOperator              For operating on ACLs.
     * @param executor                 Shared executor for executing async operations
     */
    public KafkaUserOperator(
            UserOperatorConfig config,
            KubernetesClient client,
            CertManager certManager,
            AdminApiOperator<String, List<String>> scramCredentialsOperator,
            AdminApiOperator<KafkaUserQuotas, Set<String>> quotasOperator,
            AdminApiOperator<Set<SimpleAclRule>, Set<String>> aclOperator,
            ExecutorService executor
    ) {
        this.certManager = certManager;
        this.client = client;
        this.scramCredentialsOperator = scramCredentialsOperator;
        this.quotasOperator = quotasOperator;
        this.aclOperator = aclOperator;
        this.executor = executor;
        this.config = config;

        this.selector = (config.getLabels() == null || config.getLabels().toMap().isEmpty()) ? new LabelSelector() : new LabelSelector(null, config.getLabels().toMap());
        this.passwordGenerator = new PasswordGenerator(this.config.getScramPasswordLength());
    }

    /**
     * Starts the KafkaUserOperator and the Kafka Admin API operators
     */
    public void start() {
        quotasOperator.start();
        aclOperator.start();
        scramCredentialsOperator.start();
    }

    /**
     * Stops the KafkaUserOperator and the Kafka Admin API operators
     */
    public void stop() {
        quotasOperator.stop();
        aclOperator.stop();
        scramCredentialsOperator.stop();
    }

    /**
     * Gets all usernames which should be reconciled. They are collected from the Kubernetes resources as well as from
     * the Kafka itself (based on existing ACLs, Quotas or SCRAM-SHA credentials). Querying the users also from Kafka
     * is important to ensure proper deletion.
     *
     * @param namespace     Namespace where to look for the users
     *
     * @return  Set with Users and their namespaces
     */
    public CompletionStage<Set<NamespaceAndName>> getAllUsers(String namespace) {
        // Get all users from KafkaUser resources
        CompletionStage<Set<String>> kafkaUsers = getAllKafkaUserUsernames(namespace);

        // Get the quota users
        CompletionStage<Set<String>> quotaUsers = quotasOperator.getAllUsers();

        // Get the ACL users
        CompletionStage<Set<String>> aclUsers;
        if (config.isAclsAdminApiSupported())   {
            aclUsers = aclOperator.getAllUsers();
        } else {
            aclUsers = CompletableFuture.completedFuture(Set.of());
        }

        // Get the SCRAM-SHA users
        CompletionStage<List<String>> scramUsers;
        if (!config.isKraftEnabled()) {
            scramUsers = scramCredentialsOperator.getAllUsers();
        } else {
            scramUsers = CompletableFuture.completedFuture(List.of());
        }

        return CompletableFuture.allOf(kafkaUsers.toCompletableFuture(), quotaUsers.toCompletableFuture(), aclUsers.toCompletableFuture(), scramUsers.toCompletableFuture())
                .thenApplyAsync(i -> {
                    Set<String> usernames = new HashSet<>();

                    // These CompletionStages should be complete since we were waiting for them in allOf above. So we can just getNow() the results.
                    usernames.addAll(kafkaUsers.toCompletableFuture().getNow(Set.of()));
                    usernames.addAll(quotaUsers.toCompletableFuture().getNow(Set.of()));
                    usernames.addAll(aclUsers.toCompletableFuture().getNow(Set.of()));
                    usernames.addAll(scramUsers.toCompletableFuture().getNow(List.of()));

                    return toResourceRef(namespace, usernames);
                }, executor);
    }

    /**
     * Utility method to get all usernames based on the KafkaUser Kubernetes resources
     *
     * @param namespace     Namespace where to look for the users
     *
     * @return  Set of KafkaUser resource names
     */
    private CompletionStage<Set<String>> getAllKafkaUserUsernames(String namespace)  {
        return CompletableFuture
                .supplyAsync(() -> Crds.kafkaUserOperation(client)
                                .inNamespace(namespace)
                                .withLabelSelector(selector)
                                .list(new ListOptionsBuilder().withResourceVersion("0").build())
                                .getItems()
                                .stream()
                                .map(resource -> resource.getMetadata().getName())
                                .collect(Collectors.toSet()),
                        executor);
    }

    /**
     * Utility method to convert collection of usernames to a collection of NamespaceAndName resources.
     *
     * @param namespace Namespace where these users exist
     * @param names     Name of the user
     *
     * @return  Collection with the corresponding NamespaceAndName resources
     */
    private static Set<NamespaceAndName> toResourceRef(String namespace, Collection<String> names) {
        return names.stream()
                .map(name -> new NamespaceAndName(namespace, name))
                .collect(Collectors.toSet());
    }

    /**
     * Reconciles the KafkaUser for creation, update or deletion
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param kafkaUser         KafkaUser resources with the desired user configuration.
     * @param userSecret        Secret with credentials for the user
     *
     * @return  CompletionStage which completes when the reconciliation is done
     */
    public CompletionStage<KafkaUserStatus> reconcile(Reconciliation reconciliation, KafkaUser kafkaUser, Secret userSecret)  {
        if (kafkaUser != null)  {
            // Create or update
            return createOrUpdate(reconciliation, kafkaUser, userSecret);
        } else {
            // Delete the user from everywhere with both the TLS and SCRAM-SHa name variants
            return delete(reconciliation).thenApplyAsync(i -> null, executor);
        }
    }

    /**
     * Deletes the user
     *
     * @param reconciliation    Reconciliation marker
     *
     * @return A CompletionStage
     */
    private CompletionStage<Void> delete(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String user = reconciliation.name();

        LOGGER.debugCr(reconciliation, "Deleting User {} from namespace {}", user, namespace);

        // Delete everything what can be deleted
        return CompletableFuture.allOf(
                CompletableFuture.supplyAsync(() -> client.secrets().inNamespace(namespace).withName(KafkaUserModel.getSecretName(config.getSecretPrefix(), user)).delete(), executor),
                config.isAclsAdminApiSupported() ? aclOperator.reconcile(reconciliation, KafkaUserModel.getTlsUserName(user), null).toCompletableFuture() : CompletableFuture.completedFuture(ReconcileResult.noop(null)),
                config.isAclsAdminApiSupported() ? aclOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(user), null).toCompletableFuture() : CompletableFuture.completedFuture(ReconcileResult.noop(null)),
                !config.isKraftEnabled() ? scramCredentialsOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(user), null).toCompletableFuture() : CompletableFuture.completedFuture(ReconcileResult.noop(null)),
                quotasOperator.reconcile(reconciliation, KafkaUserModel.getTlsUserName(user), null).toCompletableFuture(),
                quotasOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(user), null).toCompletableFuture()
        );
    }

    /**
     * Creates or updates the user. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param kafkaUser         KafkaUser resources with the desired user configuration.
     * @param userSecret        Secret with credentials for the user
     *
     * @return a CompletionStage
     */
    private CompletionStage<KafkaUserStatus> createOrUpdate(Reconciliation reconciliation, KafkaUser kafkaUser, Secret userSecret) {
        KafkaUserModel user;
        KafkaUserStatus userStatus = new KafkaUserStatus();

        try {
            user = KafkaUserModel.fromCrd(kafkaUser, config.getSecretPrefix(), config.isAclsAdminApiSupported(), config.isKraftEnabled());
            LOGGER.debugCr(reconciliation, "Updating User {} in namespace {}", reconciliation.name(), reconciliation.namespace());
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaUser, userStatus, e);
            return CompletableFuture.failedFuture(new ReconciliationException(userStatus, e));
        }

        // Makes sure the credentials are up-to-date. (This just updates the information inside the KafkaUserModel.
        // It does not generate the secret or update the password in Kafka. That happens only later.)
        maybeGenerateCredentials(reconciliation, user, userSecret);

        // Reconcile the user: update everything in Kafka and in the Secret
        return reconcileCredentialsQuotasAndAcls(reconciliation, user, userSecret, userStatus)
                .handleAsync((i, e) -> {
                    if (e != null)  {
                        throw new CompletionException(e);
                    } else {
                        StatusUtils.setStatusConditionAndObservedGeneration(kafkaUser, userStatus, (Throwable) null);
                        userStatus.setUsername(user.getUserName());
                        return (Void) null;
                    }
                }, executor)
                .thenApplyAsync(i -> userStatus, executor);
    }

    /**
     * Depending on the KafkaUser configuration and the user secret, this method will set or generate the credentials
     * for given user.
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Secret with existing user credentials or null if the secret doesn't exist yet
     */
    private void maybeGenerateCredentials(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret)   {
        // Generates the password or user certificate
        if (user.isScramUser()) {
            maybeGenerateScramCredentials(reconciliation, user, userSecret);
        } else if (user.isTlsUser())    {
            maybeGenerateTlsCredentials(reconciliation, user, userSecret);
        }
    }

    /**
     * Sets or generates the credentials for a SCRAM-SHA-512 user
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Secret with existing user credentials or null if the secret doesn't exist yet
     */
    private void maybeGenerateScramCredentials(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret)   {
        Secret desiredPasswordSecret = null;

        if (user.isUserWithDesiredPassword())   {
            // User is a SCRAM-SHA-512 user and requested some specific password instead of generating a random password
            desiredPasswordSecret = client.secrets().inNamespace(reconciliation.namespace()).withName(user.desiredPasswordSecretName()).get();
            if (desiredPasswordSecret == null) {
                throw new InvalidResourceException("Secret " + config.getCaCertSecretName() + " in namespace " + config.getCaNamespace() + " with requested password not found");
            }
        }

        user.maybeGeneratePassword(
                reconciliation,
                passwordGenerator,
                userSecret,
                desiredPasswordSecret
        );
    }

    /**
     * Sets or generates the credentials for a TLS user
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Secret with existing user credentials or null if the secret doesn't exist yet
     */
    private void maybeGenerateTlsCredentials(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret) {
        Secret caCert = client.secrets().inNamespace(config.getCaNamespace()).withName(config.getCaCertSecretName()).get();
        if (caCert == null) {
            throw new InvalidConfigurationException("CA certificate secret " + config.getCaCertSecretName() + " in namespace " + config.getCaNamespace() + " not found");
        }

        Secret caKey = client.secrets().inNamespace(config.getCaNamespace()).withName(config.getCaKeySecretName()).get();
        if (caKey == null) {
            throw new InvalidConfigurationException("CA certificate secret " + config.getCaKeySecretName() + " in namespace " + config.getCaNamespace() + " not found");
        }

        user.maybeGenerateCertificates(
                reconciliation,
                certManager,
                passwordGenerator,
                caCert,
                caKey,
                userSecret,
                config.getClientsCaValidityDays(),
                config.getClientsCaRenewalDays(),
                config.getMaintenanceWindows(),
                Clock.systemUTC()
        );
    }

    /**
     * Reconciles the credentials, quotas and ACLs
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param userSecret        Current user secret
     * @param userStatus        Status subresource of the KafkaUser custom resource
     *
     * @return                  CompletionStage describing the result
     */
    private CompletionStage<Void> reconcileCredentialsQuotasAndAcls(Reconciliation reconciliation, KafkaUserModel user, Secret userSecret, KafkaUserStatus userStatus)   {
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
        CompletionStage<ReconcileResult<String>> scramCredentialsFuture;
        if (config.isKraftEnabled()) {
            // SCRAM-SHA authentication is currently not supported when KRaft is used
            scramCredentialsFuture = CompletableFuture.completedFuture(ReconcileResult.noop(null));
        } else {
            scramCredentialsFuture = scramCredentialsOperator.reconcile(reconciliation, user.getName(), user.getScramSha512Password());
        }

        // Quotas need to reconciled for both regular and TLS username. It will be (possibly) set for one user and deleted for the other
        CompletionStage<ReconcileResult<KafkaUserQuotas>> tlsQuotasFuture = quotasOperator.reconcile(reconciliation, KafkaUserModel.getTlsUserName(reconciliation.name()), tlsQuotas);
        CompletionStage<ReconcileResult<KafkaUserQuotas>> quotasFuture = quotasOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(reconciliation.name()), scramOrNoneQuotas);

        // Reconcile the user secret generated by the user operator with the credentials
        CompletionStage<ReconcileResult<Secret>> userSecretFuture = reconcileUserSecret(reconciliation, user, userSecret, userStatus);

        // ACLs need to reconciled for both regular and TLS username. It will be (possibly) set for one user and deleted for the other
        CompletionStage<ReconcileResult<Set<SimpleAclRule>>> aclsTlsUserFuture;
        CompletionStage<ReconcileResult<Set<SimpleAclRule>>> aclsScramUserFuture;

        if (config.isAclsAdminApiSupported()) {
            aclsTlsUserFuture = aclOperator.reconcile(reconciliation, KafkaUserModel.getTlsUserName(reconciliation.name()), tlsAcls);
            aclsScramUserFuture = aclOperator.reconcile(reconciliation, KafkaUserModel.getScramUserName(reconciliation.name()), scramOrNoneAcls);
        } else {
            aclsTlsUserFuture = CompletableFuture.completedFuture(ReconcileResult.noop(null));
            aclsScramUserFuture = CompletableFuture.completedFuture(ReconcileResult.noop(null));
        }

        return CompletableFuture.allOf(
                scramCredentialsFuture.toCompletableFuture(),
                tlsQuotasFuture.toCompletableFuture(),
                quotasFuture.toCompletableFuture(),
                aclsTlsUserFuture.toCompletableFuture(),
                aclsScramUserFuture.toCompletableFuture(),
                userSecretFuture.toCompletableFuture()
        );
    }

    /**
     * Reconciles the Kubernetes secret with the generated credentials and sets the secret name in the KafkaUser status subresource
     *
     * @param reconciliation    Unique identification for the reconciliation
     * @param user              Model describing the KafkaUser
     * @param currentSecret     The current user secret
     * @param userStatus        Status subresource of the KafkaUser custom resource
     *
     * @return                  CompletionStage describing the result
     */
    private CompletionStage<ReconcileResult<Secret>> reconcileUserSecret(Reconciliation reconciliation, KafkaUserModel user, Secret currentSecret, KafkaUserStatus userStatus) {
        return CompletableFuture.supplyAsync(() -> {
            String namespace = reconciliation.namespace();
            String name = user.getSecretName();
            Secret desiredSecret = user.generateSecret();

            if (desiredSecret != null)  {
                if (currentSecret != null)  {
                    // Both secrets exist => if they differ we patch them, if not we just continue
                    if (new ResourceDiff<>(reconciliation, "Secret", name, currentSecret, desiredSecret, ResourceDiff.DEFAULT_IGNORABLE_PATHS).isEmpty()) {
                        // Secrets are identical
                        LOGGER.debugCr(reconciliation, "Secret {}/{} exist, and is identical", namespace, name);
                        userStatus.setSecret(desiredSecret.getMetadata().getName());
                        return ReconcileResult.noop(desiredSecret);
                    } else {
                        // Secrets differ
                        LOGGER.debugCr(reconciliation, "Secret {}/{} exist, patching it", namespace, name);
                        client.secrets().inNamespace(namespace).resource(desiredSecret).createOrReplace();
                        userStatus.setSecret(desiredSecret.getMetadata().getName());
                        return ReconcileResult.patched(desiredSecret);
                    }
                } else {
                    LOGGER.debugCr(reconciliation, "Secret {}/{} does not exist, creating it", namespace, name);
                    client.secrets().inNamespace(namespace).resource(desiredSecret).createOrReplace();
                    userStatus.setSecret(desiredSecret.getMetadata().getName());
                    return ReconcileResult.created(desiredSecret);
                }
            } else {
                if (currentSecret != null)  {
                    LOGGER.debugCr(reconciliation, "Secret {}/{} exist, deleting it", namespace, name);
                    client.secrets().inNamespace(namespace).withName(name).delete();
                    return ReconcileResult.deleted();
                } else {
                    LOGGER.debugCr(reconciliation, "Secret {}/{} does not exist, noop", namespace, name);
                    return ReconcileResult.noop(null);
                }
            }
        }, executor);
    }
}
