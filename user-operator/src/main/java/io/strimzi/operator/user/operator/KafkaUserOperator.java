/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
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
    private final CrdOperator crdOperator;
    private final SecretOperator secretOperations;
    private final SimpleAclOperator aclOperations;
    private final CertManager certManager;
    private final String caName;
    private final String caNamespace;
    private final ScramShaCredentialsOperator scramShaCredentialOperator;
    private PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    /**
     * @param vertx The Vertx instance
     * @param certManager For managing certificates
     * @param crdOperator For operating on Custom Resources
     * @param secretOperations For operating on Secrets
     */
    public KafkaUserOperator(Vertx vertx,
                             CertManager certManager,
                             CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> crdOperator,
                             SecretOperator secretOperations,
                             ScramShaCredentialsOperator scramShaCredentialOperator,
                             SimpleAclOperator aclOperations, String caName, String caNamespace) {
        this.vertx = vertx;
        this.certManager = certManager;
        this.secretOperations = secretOperations;
        this.scramShaCredentialOperator = scramShaCredentialOperator;
        this.crdOperator = crdOperator;
        this.aclOperations = aclOperations;
        this.caName = caName;
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
     * @param clientsCa Secret with the Clients CA
     * @param userSecret User secret (if it exists, null otherwise)
     * @param handler Completion handler
     */
    protected void createOrUpdate(Reconciliation reconciliation, KafkaUser kafkaUser, Secret clientsCa, Secret userSecret, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String userName = reconciliation.name();
        KafkaUserModel user;
        try {
            user = KafkaUserModel.fromCrd(certManager, passwordGenerator, kafkaUser, clientsCa, userSecret);
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
            return;
        }

        log.debug("{}: Updating User", reconciliation, userName, namespace);
        Secret desired = user.generateSecret();
        CompositeFuture.join(
                scramShaCredentialOperator.reconcile(user.getName(), desired != null ? desired.getData().get("password") : null),
                secretOperations.reconcile(namespace, user.getSecretName(), desired),
                aclOperations.reconcile(user.getUserName(), user.getSimpleAclRules()))
                .map((Void) null).setHandler(handler);
    }

    /**
     * Deletes the user
     *
     * @param handler Completion handler
     */
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String user = reconciliation.name();

        // TODO delete the user config

        log.debug("{}: Deleting User", reconciliation, user, namespace);
        CompositeFuture.join(secretOperations.reconcile(namespace, KafkaUserModel.getSecretName(user), null),
                aclOperations.reconcile(KafkaUserModel.getUserName(user), null),
                scramShaCredentialOperator.reconcile(KafkaUserModel.getUserName(user), null))
            .map((Void) null).setHandler(handler);
    }

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaUser}) in the given namespace with the given name and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
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
                        Secret clientsCa = secretOperations.get(caNamespace, caName);
                        Secret userSecret = secretOperations.get(namespace, KafkaUserModel.getSecretName(name));

                        createOrUpdate(reconciliation, cr, clientsCa, userSecret, createResult -> {
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
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     *
     * @param trigger A description of the triggering event (timer or watch), used for logging
     * @param namespace The namespace
     * @param selector The labels used to select the resources
     */
    public final CountDownLatch reconcileAll(String trigger, String namespace, Labels selector) {
        List<KafkaUser> desiredResources = crdOperator.list(namespace, selector);
        Set<String> desiredNames = desiredResources.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): desired resources with labels {}: {}", RESOURCE_KIND, trigger, selector, desiredNames);

        Labels resourceSelector = selector.withKind(RESOURCE_KIND);
        List<? extends HasMetadata> resources = getResources(namespace, resourceSelector);
        Set<String> resourceNames = resources.stream()
                .filter(r -> !r.getKind().equals(RESOURCE_KIND)) // exclude desired resource
                .map(r -> ((HasMetadata) r).getMetadata().getName())
                .collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): Other resources with labels {}: {}", RESOURCE_KIND, trigger, resourceSelector, resourceNames);

        CountDownLatch outerLatch = new CountDownLatch(1);

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
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
                    desiredNames.addAll((Collection<? extends String>) res.result());
                    desiredNames.addAll(resourceNames);
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
     * Gets all resources relevant to KafkaUser
     *
     * @param namespace Namespace where to search for resources
     * @param selector  Labels which the resources should have
     * @return
     */
    private List<HasMetadata> getResources(String namespace, Labels selector) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(secretOperations.list(namespace, selector));
        return result;
    }

    /**
     * Create Kubernetes watch for KafkaUser resources
     *
     * @param namespace Namespace where to watch for users
     * @param selector  Labels which the Users should match
     * @param onClose   Callbeck called when the watch is closed
     *
     * @return
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
            }, result.completer()
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
}
