/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.Credential;
import io.strimzi.api.kafka.model.status.CredentialBuilder;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Operator for a Kafka Users.
 */
public class KafkaUserOperator {
    private static final Logger log = LogManager.getLogger(KafkaUserOperator.class.getName());
    private static final int LOCK_TIMEOUT_MS = 10;
    private static final String RESOURCE_KIND = "KafkaUser";
    private final Vertx vertx;
    private final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> crdOperator;
    private final SecretOperator secretOperations;
    private final SimpleAclOperator aclOperations;
    private final CertManager certManager;
    private final String caCertName;
    private final String caKeyName;
    private final String caNamespace;
    private final ScramShaCredentialsOperator scramShaCredentialOperator;
    private PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    /**
     * @param vertx The Vertx instance.
     * @param certManager For managing certificates.
     * @param crdOperator For operating on Custom Resources.
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
                             SecretOperator secretOperations,
                             ScramShaCredentialsOperator scramShaCredentialOperator,
                             SimpleAclOperator aclOperations, String caCertName, String caKeyName, String caNamespace) {
        this.vertx = vertx;
        this.certManager = certManager;
        this.secretOperations = secretOperations;
        this.scramShaCredentialOperator = scramShaCredentialOperator;
        this.crdOperator = crdOperator;
        this.aclOperations = aclOperations;
        this.caCertName = caCertName;
        this.caKeyName = caKeyName;
        this.caNamespace = caNamespace;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code namespace} and
     * cluster {@code name}
     *
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    private final String getLockName(String namespace, String name) {
        return "lock::" + namespace + "::" + RESOURCE_KIND + "::" + name;
    }

    /**
     * Creates or updates the user. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param reconciliation Unique identification for the reconciliation
     * @param kafkaUser KafkaUser resources with the desired user configuration.
     * @param clientsCaCert Secret with the Clients CA cert
     * @param clientsCaCert Secret with the Clients CA key
     * @param userSecret User secret (if it exists, null otherwise)
     * @param handler Completion handler
     */
    protected void createOrUpdate(Reconciliation reconciliation, KafkaUser kafkaUser, Secret clientsCaCert, Secret clientsCaKey, Secret userSecret, Handler<AsyncResult<Void>> handler) {
        Future<Void> createOrUpdateFuture = Future.future();
        String namespace = reconciliation.namespace();
        String userName = reconciliation.name();
        KafkaUserModel user;
        try {
            user = KafkaUserModel.fromCrd(certManager, passwordGenerator, kafkaUser, clientsCaCert, clientsCaKey, userSecret);
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
            return;
        }

        log.debug("{}: Updating User {} in namespace {}", reconciliation, userName, namespace);
        Secret desired = user.generateSecret();
        String password = null;

        if (desired != null && desired.getData().get("password") != null)   {
            password = new String(Base64.getDecoder().decode(desired.getData().get("password")), Charset.forName("US-ASCII"));
        }

        Set<SimpleAclRule> tlsAcls = null;
        Set<SimpleAclRule> scramAcls = null;

        if (user.isTlsUser())   {
            tlsAcls = user.getSimpleAclRules();
        } else if (user.isScramUser())  {
            scramAcls = user.getSimpleAclRules();
        }

        KafkaUserStatus userStatus = new KafkaUserStatus();
        CompositeFuture.join(
                scramShaCredentialOperator.reconcile(user.getName(), password),
                reconcileSecretAndSetStatus(namespace, user, desired, userStatus),
                aclOperations.reconcile(KafkaUserModel.getTlsUserName(userName), tlsAcls),
                aclOperations.reconcile(KafkaUserModel.getScramUserName(userName), scramAcls))
                .compose(reconciliationResult -> {
                    Condition readyCondition;
                    if (kafkaUser.getMetadata().getGeneration() != null)    {
                        userStatus.setObservedGeneration(kafkaUser.getMetadata().getGeneration());
                    }

                    if (reconciliationResult.succeeded()) {
                        readyCondition = new ConditionBuilder()
                                .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                                .withNewType("Ready")
                                .withNewStatus("True")
                                .build();
                    } else {
                        readyCondition = new ConditionBuilder()
                                .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                                .withNewType("NotReady")
                                .withNewStatus("True")
                                .withNewReason(reconciliationResult.cause().getClass().getSimpleName())
                                .withNewMessage(reconciliationResult.cause().getMessage())
                                .build();
                    }

                    userStatus.setUsername(user.getName());
                    userStatus.setConditions(Collections.singletonList(readyCondition));

                    updateStatus(kafkaUser, reconciliation, userStatus).setHandler(statusResult -> {
                        if (statusResult.succeeded())    {
                            log.debug("Status for {} is up to date", kafkaUser.getMetadata().getName());
                        } else {
                            log.error("Failed to set status for {}. {}", kafkaUser.getMetadata().getName(), statusResult.cause().getMessage());
                        }

                        // If both features succeeded, createOrUpdate succeeded as well
                        // If one or both of them failed, we prefer the reconciliation failure as the main error
                        if (reconciliationResult.succeeded() && statusResult.succeeded())    {
                            createOrUpdateFuture.complete();
                        } else if (reconciliationResult.failed())    {
                            createOrUpdateFuture.fail(reconciliationResult.cause());
                        } else {
                            createOrUpdateFuture.fail(statusResult.cause());
                        }
                    });
                    return createOrUpdateFuture;
                }).map((Void) null).setHandler(handler);
    }

    private Future<ReconcileResult<Secret>> reconcileSecretAndSetStatus(String namespace, KafkaUserModel user, Secret desired, KafkaUserStatus userStatus) {
        return secretOperations.reconcile(namespace, user.getSecretName(), desired).compose(ar -> {
            Credential credential = null;
            if (desired != null) {
                credential = new CredentialBuilder()
                        .withSecret(desired.getMetadata().getName())
                        .build();
            }
            userStatus.setCredentials(credential);
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

        crdOperator.getAsync(kafkaUserAssembly.getMetadata().getNamespace(), kafkaUserAssembly.getMetadata().getName()).setHandler(getRes -> {
            if (getRes.succeeded()) {
                KafkaUser user = getRes.result();

                if (user != null) {
                    if ("kafka.strimzi.io/v1alpha1".equals(user.getApiVersion())) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, user.getApiVersion());
                        updateStatusFuture.complete();
                    } else {
                        KafkaUserStatus currentStatus = user.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            KafkaUser resourceWithNewStatus = new KafkaUserBuilder(user).withStatus(desiredStatus).build();

                            crdOperator.updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
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
     * @param handler Completion handler
     */
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String user = reconciliation.name();
        log.debug("{}: Deleting User", reconciliation, user, namespace);
        CompositeFuture.join(secretOperations.reconcile(namespace, KafkaUserModel.getSecretName(user), null),
                aclOperations.reconcile(KafkaUserModel.getTlsUserName(user), null),
                aclOperations.reconcile(KafkaUserModel.getScramUserName(user), null),
                scramShaCredentialOperator.reconcile(KafkaUserModel.getScramUserName(user), null))
            .map((Void) null).setHandler(handler);
    }

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaUser}) in the given namespace with the given name and
     * comparing with the corresponding resource.
     * @param reconciliation The reconciliation.
     * @param handler The result handler.
     */
    public final void reconcile(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT_MS, res -> {
            if (res.succeeded()) {
                log.debug("{}: Lock {} acquired", reconciliation, lockName);
                Lock lock = res.result();

                try {
                    KafkaUser cr = (KafkaUser) crdOperator.get(namespace, name);

                    if (cr != null) {
                        log.info("{}: User {} should be created or updated", reconciliation, name);
                        Secret clientsCaCert = secretOperations.get(caNamespace, caCertName);
                        Secret clientsCaKey = secretOperations.get(caNamespace, caKeyName);
                        Secret userSecret = secretOperations.get(namespace, KafkaUserModel.getSecretName(name));

                        createOrUpdate(reconciliation, cr, clientsCaCert, clientsCaKey, userSecret, createResult -> {
                            lock.release();
                            log.debug("{}: Lock {} released", reconciliation, lockName);
                            if (createResult.failed()) {
                                log.error("{}: createOrUpdate failed", reconciliation, createResult.cause());
                            } else {
                                handler.handle(createResult);
                            }
                        });
                    } else {
                        log.info("{}: User {} should be deleted", reconciliation, name);
                        delete(reconciliation, deleteResult -> {
                            if (deleteResult.succeeded())   {
                                log.info("{}: User {} deleted", reconciliation, name);
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                handler.handle(deleteResult);
                            } else {
                                log.error("{}: Deletion of user {} failed", reconciliation, name, deleteResult.cause());
                                lock.release();
                                log.debug("{}: Lock {} released", reconciliation, lockName);
                                handler.handle(deleteResult);
                            }
                        });
                    }
                } catch (Throwable ex) {
                    lock.release();
                    log.error("{}: Reconciliation failed", reconciliation, ex);
                    log.debug("{}: Lock {} released", reconciliation, lockName);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.warn("{}: Failed to acquire lock {}.", reconciliation, lockName);
            }
        });
    }

    /**
     * Reconcile User resources in the given namespace having the given selector.
     * Reconciliation works by getting the KafkaUSer custom resources in the given namespace with the given selector and
     * comparing with the corresponding resource.
     *
     * @param trigger A description of the triggering event (timer or watch), used for logging
     * @param namespace The namespace
     * @param selector The labels used to select the resources
     * @return A latch for awaiting the reconciliation.
     */
    public final CountDownLatch reconcileAll(String trigger, String namespace, Labels selector) {
        List<KafkaUser> desiredResources = crdOperator.list(namespace, selector);
        Set<String> desiredNames = desiredResources.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): desired resources with labels {}: {}", RESOURCE_KIND, trigger, selector, desiredNames);

        CountDownLatch outerLatch = new CountDownLatch(1);

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<Set<String>>executeBlocking(
            future -> {
                try {
                    Set<String> usersWithAcls = aclOperations.getUsersWithAcls();
                    future.complete(usersWithAcls);
                } catch (Throwable t) {
                    future.failed();
                }
            }, res -> {
                if (res.succeeded()) {
                    log.debug("reconcileAll({}, {}): User with ACLs: {}", RESOURCE_KIND, trigger, res.result());
                    desiredNames.addAll(res.result());
                    desiredNames.addAll(scramShaCredentialOperator.list());

                    // We use a latch so that callers (specifically, test callers) know when the reconciliation is complete
                    // Using futures would be more complex for no benefit
                    AtomicInteger counter = new AtomicInteger(desiredNames.size());

                    for (String name : desiredNames) {
                        Reconciliation reconciliation = new Reconciliation(trigger, ResourceType.USER, namespace, name);
                        reconcile(reconciliation, result -> {
                            handleResult(reconciliation, result);
                            if (counter.getAndDecrement() == 0) {
                                outerLatch.countDown();
                            }
                        });
                    }
                } else {
                    log.error("Error while getting users with ACLs");
                }
                return;
            });

        return outerLatch;
    }

    /**
     * Create Kubernetes watch for KafkaUser resources.
     *
     * @param namespace Namespace where to watch for users.
     * @param selector Labels which the Users should match.
     * @param onClose Callback called when the watch is closed.
     *
     * @return A future which completes when the watcher has been created.
     */
    public Future<Watch> createWatch(String namespace, Labels selector, Consumer<KubernetesClientException> onClose) {
        Future<Watch> result = Future.future();
        vertx.<Watch>executeBlocking(
            future -> {
                Watch watch = crdOperator.watch(namespace, selector, new Watcher<KafkaUser>() {
                    @Override
                    public void eventReceived(Action action, KafkaUser crd) {
                        String name = crd.getMetadata().getName();
                        switch (action) {
                            case ADDED:
                            case DELETED:
                            case MODIFIED:
                                Reconciliation reconciliation = new Reconciliation("watch", ResourceType.USER, namespace, name);
                                log.info("{}: {} {} in namespace {} was {}", reconciliation, RESOURCE_KIND, name, namespace, action);
                                reconcile(reconciliation, result -> {
                                    handleResult(reconciliation, result);
                                });
                                break;
                            case ERROR:
                                log.error("Failed {} {} in namespace{} ", RESOURCE_KIND, name, namespace);
                                reconcileAll("watch error", namespace, selector);
                                break;
                            default:
                                log.error("Unknown action: {} in namespace {}", name, namespace);
                                reconcileAll("watch unknown", namespace, selector);
                        }
                    }

                    @Override
                    public void onClose(KubernetesClientException e) {
                        onClose.accept(e);
                    }
                });
                future.complete(watch);
            }, result
        );
        return result;
    }

    /**
     * Log the reconciliation outcome.
     */
    private void handleResult(Reconciliation reconciliation, AsyncResult<Void> result) {
        if (result.succeeded()) {
            log.info("{}: User reconciled", reconciliation);
        } else {
            Throwable cause = result.cause();
            log.warn("{}: Failed to reconcile", reconciliation, cause);
        }
    }

    private Date dateSupplier() {
        return new Date();
    }
}
