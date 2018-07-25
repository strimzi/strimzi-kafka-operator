/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.SecretCertProvider;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.operator.resource.AbstractWatchableResourceOperator;
import io.strimzi.operator.cluster.operator.resource.SecretOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <p>Abstract assembly creation, update, read, deletion, etc.</p>
 *
 * <p>An assembly is a collection of Kubernetes resources of various types
 * (e.g. Services, StatefulSets, Deployments etc) which operate together to provide some functionality.</p>
 *
 * <p>This class manages a per-assembly locking strategy so only one operation per assembly
 * can proceed at once.</p>
 */
public abstract class AbstractAssemblyOperator<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/, D extends Doneable<T>, R extends Resource<T, D>> {

    private static final Logger log = LogManager.getLogger(AbstractAssemblyOperator.class.getName());

    protected static final int LOCK_TIMEOUT_MS = 10;
    protected static final int CERTS_EXPIRATION_DAYS = 365;

    protected final Vertx vertx;
    protected final boolean isOpenShift;
    protected final AssemblyType assemblyType;
    protected final AbstractWatchableResourceOperator<C, T, L, D, R> resourceOperator;
    protected final SecretOperator secretOperations;
    protected final CertManager certManager;
    private final String kind;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift True iff running on OpenShift
     * @param assemblyType Assembly type
     * @param resourceOperator For operating on the desired resource
     */
    protected AbstractAssemblyOperator(Vertx vertx, boolean isOpenShift, AssemblyType assemblyType,
                                       CertManager certManager,
                                       AbstractWatchableResourceOperator<C, T, L, D, R> resourceOperator,
                                       SecretOperator secretOperations) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.assemblyType = assemblyType;
        this.kind = assemblyType.name;
        this.resourceOperator = resourceOperator;
        this.certManager = certManager;
        this.secretOperations = secretOperations;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code assemblyType}, {@code namespace} and
     * cluster {@code name}
     * @param assemblyType The type of cluster
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    protected final String getLockName(AssemblyType assemblyType, String namespace, String name) {
        return "lock::" + namespace + "::" + assemblyType + "::" + name;
    }

    /**
     * Subclasses implement this method to create or update the cluster. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param reconciliation Unique identification for the reconciliation
     * @param assemblyResource Resources with the desired cluster configuration.
     * @param assemblySecrets Secrets related to the cluster
     * @param handler Completion handler
     */
    protected abstract void createOrUpdate(Reconciliation reconciliation, T assemblyResource, List<Secret> assemblySecrets, Handler<AsyncResult<Void>> handler);

    /**
     * Subclasses implement this method to delete the cluster.
     * @param handler Completion handler
     */
    protected abstract void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler);

    /**
     * The name of the given {@code resource}, as read from its metadata.
     * @param resource The resource
     */
    protected static String name(HasMetadata resource) {
        if (resource != null) {
            ObjectMeta metadata = resource.getMetadata();
            if (metadata != null) {
                return metadata.getName();
            }
        }
        return null;
    }

    private final void reconcileClusterCa(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                String clusterCaName = AbstractModel.getClusterCaName(reconciliation.assemblyName());

                if (secretOperations.get(reconciliation.namespace(), clusterCaName) == null) {
                    log.debug("{}: Generating cluster CA certificate {}", reconciliation, clusterCaName);
                    File clusterCAkeyFile = null;
                    File clusterCAcertFile = null;
                    try {
                        clusterCAkeyFile = File.createTempFile("tls", "cluster-ca-key");
                        clusterCAcertFile = File.createTempFile("tls", "cluster-ca-cert");

                        Subject sbj = new Subject();
                        sbj.setOrganizationName("io.strimzi");
                        sbj.setCommonName("cluster-ca");

                        certManager.generateSelfSignedCert(clusterCAkeyFile, clusterCAcertFile, sbj, CERTS_EXPIRATION_DAYS);

                        SecretCertProvider secretCertProvider = new SecretCertProvider();
                        Secret secret = secretCertProvider.createSecret(reconciliation.namespace(), clusterCaName,
                                "cluster-ca.key", "cluster-ca.crt",
                                clusterCAkeyFile, clusterCAcertFile, Collections.emptyMap());

                        secretOperations.reconcile(reconciliation.namespace(), clusterCaName, secret)
                                .compose(future::complete, future);
                    } catch (Throwable e) {
                        future.fail(e);
                    } finally {
                        if (clusterCAkeyFile != null)
                            clusterCAkeyFile.delete();
                        if (clusterCAcertFile != null)
                            clusterCAcertFile.delete();
                    }
                    log.debug("{}: End generating cluster CA {}", reconciliation, clusterCaName);
                } else {
                    log.debug("{}: The cluster CA {} already exists", reconciliation, clusterCaName);
                    future.complete();
                }
            }, true,
            res -> {
                if (res.succeeded())
                    handler.handle(Future.succeededFuture());
                else
                    handler.handle(Future.failedFuture(res.cause()));
            }
        );
    }

    private final void deleteClusterCa(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                String clusterCaName = AbstractModel.getClusterCaName(reconciliation.assemblyName());

                if (secretOperations.get(reconciliation.namespace(), clusterCaName) != null) {
                    log.debug("{}: Deleting cluster CA certificate {}", reconciliation, clusterCaName);
                    secretOperations.reconcile(reconciliation.namespace(), clusterCaName, null)
                            .compose(future::complete, future);
                    log.debug("{}: Cluster CA {} deleted", reconciliation, clusterCaName);
                } else {
                    log.debug("{}: The cluster CA {} doesn't exist", reconciliation, clusterCaName);
                    future.complete();
                }
            }, true,
            res -> {
                if (res.succeeded())
                    handler.handle(Future.succeededFuture());
                else
                    handler.handle(Future.failedFuture(res.cause()));
            }
        );
    }

    /**
     * Reconcile assembly resources in the given namespace having the given {@code assemblyName}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaAssembly}) in the given namespace with the given assemblyName and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(Reconciliation, T, List, Handler) created or updated} if ConfigMap is without same-named resources</li>
     * <li>An assembly will be {@linkplain #delete(Reconciliation, Handler) deleted} if resources without same-named ConfigMap</li>
     * </ul>
     */
    public final void reconcileAssembly(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String assemblyName = reconciliation.assemblyName();
        final String lockName = getLockName(assemblyType, namespace, assemblyName);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT_MS, res -> {
            if (res.succeeded()) {
                log.debug("{}: Lock {} acquired", reconciliation, lockName);
                Lock lock = res.result();

                try {
                    // get CustomResource and related resources for the specific cluster
                    T cr = resourceOperator.get(namespace, assemblyName);

                    if (cr != null) {
                        log.info("{}: Assembly {} should be created or updated", reconciliation, assemblyName);
                        reconcileClusterCa(reconciliation, certResult -> {

                            Labels labels = Labels.forCluster(assemblyName);
                            List<Secret> secrets = secretOperations.list(namespace, labels);
                            secrets.add(secretOperations.get(namespace, AbstractModel.getClusterCaName(assemblyName)));

                            if (certResult.succeeded()) {
                                createOrUpdate(reconciliation, cr, secrets, createResult -> {
                                    lock.release();
                                    log.debug("{}: Lock {} released", reconciliation, lockName);
                                    if (createResult.failed()) {
                                        if (createResult.cause() instanceof InvalidConfigMapException) {
                                            log.error(createResult.cause().getMessage());
                                        } else {
                                            log.error("{}: createOrUpdate failed", reconciliation, createResult.cause());
                                        }
                                    } else {
                                        handler.handle(createResult);
                                    }
                                });
                            } else {
                                log.error("{}: reconcileClusterCa failed", reconciliation, certResult.cause());
                                lock.release();
                            }
                        });
                    } else {
                        log.info("{}: Assembly {} should be deleted", reconciliation, assemblyName);
                        delete(reconciliation, deleteResult -> {
                            if (deleteResult.succeeded())   {
                                log.info("{}: Assembly {} deleted", reconciliation, assemblyName);

                                deleteClusterCa(reconciliation, caDeleteResult -> {
                                    lock.release();
                                    log.debug("{}: Lock {} released", reconciliation, lockName);
                                    handler.handle(caDeleteResult);
                                });
                            } else {
                                log.error("{}: Deletion of assembly {} failed", reconciliation, assemblyName, deleteResult.cause());
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
     * Reconcile assembly resources in the given namespace having the given selector.
     * Reconciliation works by getting the assembly ConfigMaps in the given namespace with the given selector and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     * <ul>
     * <li>An assembly will be {@linkplain #createOrUpdate(Reconciliation, T, List, Handler) created} for all ConfigMaps without same-named resources</li>
     * <li>An assembly will be {@linkplain #delete(Reconciliation, Handler) deleted} for all resources without same-named ConfigMaps</li>
     * </ul>
     *
     * @param trigger A description of the triggering event (timer or watch), used for logging
     * @param namespace The namespace
     */
    public final CountDownLatch reconcileAll(String trigger, String namespace) {

        // get ConfigMaps with kind=cluster&type=kafka (or connect, or connect-s2i) for the corresponding cluster type
        List<T> desiredResources = resourceOperator.list(namespace, Labels.EMPTY);
        Set<String> desiredNames = desiredResources.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): desired resources with labels {}: {}", assemblyType, trigger, Labels.EMPTY, desiredNames);

        // get resources with kind=cluster&type=kafka (or connect, or connect-s2i)
        Labels resourceSelector = Labels.EMPTY.withKind(assemblyType.name);
        List<? extends HasMetadata> resources = getResources(namespace, resourceSelector);
        // now extract the cluster name from those
        Set<String> resourceNames = resources.stream()
                .filter(r -> !r.getKind().equals(kind)) // exclude desired resource
                .map(Labels::cluster)
                .collect(Collectors.toSet());
        log.debug("reconcileAll({}, {}): Other resources with labels {}: {}", assemblyType, trigger, resourceSelector, resourceNames);

        desiredNames.addAll(resourceNames);

        // We use a latch so that callers (specifically, test callers) know when the reconciliation is complete
        // Using futures would be more complex for no benefit
        CountDownLatch latch = new CountDownLatch(desiredNames.size());

        for (String name: desiredNames) {
            Reconciliation reconciliation = new Reconciliation(trigger, assemblyType, namespace, name);
            reconcileAssembly(reconciliation, result -> {
                handleResult(reconciliation, result);
                latch.countDown();
            });
        }

        return latch;
    }

    /**
     * Gets all the assembly resources (for all assemblies) in the given namespace.
     * Assembly resources (e.g. the {@code KafkaAssembly} resource) may be included in the result.
     * @param namespace The namespace
     * @return The matching resources.
     */
    protected abstract List<HasMetadata> getResources(String namespace, Labels selector);

    public Future<Watch> createWatch(String namespace, Consumer<KubernetesClientException> onClose) {
        Future<Watch> result = Future.future();
        vertx.<Watch>executeBlocking(
            future -> {
                Watch watch = resourceOperator.watch(namespace, new Watcher<T>() {
                    @Override
                    public void eventReceived(Action action, T cm) {
                        String name = cm.getMetadata().getName();
                        switch (action) {
                            case ADDED:
                            case DELETED:
                            case MODIFIED:
                                Reconciliation reconciliation = new Reconciliation("watch", assemblyType, namespace, name);
                                log.info("{}: {} {} in namespace {} was {}", reconciliation, kind, name, namespace, action);
                                reconcileAssembly(reconciliation, result -> {
                                    handleResult(reconciliation, result);
                                });
                                break;
                            case ERROR:
                                log.error("Failed {} {} in namespace{} ", kind, name, namespace);
                                reconcileAll("watch error", namespace);
                                break;
                            default:
                                log.error("Unknown action: {} in namespace {}", name, namespace);
                                reconcileAll("watch unknown", namespace);
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
            log.info("{}: Assembly reconciled", reconciliation);
        } else {
            Throwable cause = result.cause();
            if (cause instanceof InvalidConfigMapException) {
                log.warn("{}: Failed to reconcile {}", reconciliation, cause.getMessage());
            } else {
                log.warn("{}: Failed to reconcile", reconciliation, cause);
            }
        }
    }
}
