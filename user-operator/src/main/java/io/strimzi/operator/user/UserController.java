/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.controller.AbstractControllerLoop;
import io.strimzi.operator.common.controller.ControllerQueue;
import io.strimzi.operator.common.controller.ReconciliationLockManager;
import io.strimzi.operator.common.controller.SimplifiedReconciliation;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.user.operator.KafkaUserOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User controller is responsible for queueing the reconciliations of the Kafka Users. It does so by watching for the
 * Kubernetes events and triggering the periodical reconciliations. The actual processing of the events is done by the
 * controller loop class.
 */
public class UserController {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(UserController.class);
    private static final String RESOURCE_KIND = "KafkaUser";
    private static final long DEFAULT_RESYNC_PERIOD = 5 * 60 * 1_000L; // 5 minutes by default

    private final KafkaUserOperator userOperator;
    private final ControllerMetricsHolder metrics;
    private final ControllerQueue workQueue;
    private final List<UserControllerLoop> threadPool;

    private final String watchedNamespace;
    private final String secretPrefix;
    private final long reconcileIntervalMs;
    private final long operationTimeoutMs;

    private final SharedIndexInformer<Secret> secretInformer;
    private final SharedIndexInformer<KafkaUser> userInformer;

    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Creates the User controller responsible for controlling users in a single namespace
     *
     * @param config          User Operator configuration
     * @param client          Kubernetes client
     * @param userOperator    The User Operator which encapsulates the logic for updating the users
     * @param metricsProvider Metrics provider for handling metrics
     */
    public UserController(UserOperatorConfig config, KubernetesClient client, KafkaUserOperator userOperator, MetricsProvider metricsProvider) {
        this.userOperator = userOperator;

        // Store some useful settings into local fields
        this.watchedNamespace = config.getNamespace();
        this.secretPrefix = config.getSecretPrefix();
        this.reconcileIntervalMs = config.getReconciliationIntervalMs();
        this.operationTimeoutMs = config.getOperationTimeoutMs();

        // User selector is used to select the KafkaUser resources
        Map<String, String> userSelector = (config.getLabels() == null || config.getLabels().toMap().isEmpty()) ? Map.of() : config.getLabels().toMap();

        // Selector for the secrets contains the original KafkaUSer selector adn the Strimzi Kind label
        Map<String, String> secretSelector = new HashMap<>(userSelector.size() + 1);
        secretSelector.putAll(userSelector);
        secretSelector.put("strimzi.io/kind", RESOURCE_KIND);

        // Set up the metrics holder
        this.metrics = new ControllerMetricsHolder(RESOURCE_KIND, Labels.fromMap(userSelector), metricsProvider);

        // Set up the work queue
        this.workQueue = new ControllerQueue(config.getWorkQueueSize(), this.metrics);

        // Secret informer and lister is used to get events about Secrets and get Secrets quickly
        this.secretInformer = client.secrets().inNamespace(watchedNamespace).withLabels(secretSelector).inform();
        Lister<Secret> secretLister = new Lister<>(secretInformer.getIndexer());

        // KafkaUser informer and lister is used to get events about Users and get Users quickly
        this.userInformer = Crds.kafkaUserOperation(client).inNamespace(watchedNamespace).withLabels(userSelector).inform();
        Lister<KafkaUser> userLister = new Lister<>(userInformer.getIndexer());

        // Creates the scheduled executor service used for periodical reconciliations and progress warnings
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "UserControllerScheduledExecutor"));

        // Create the reconciliation lock manager
        ReconciliationLockManager lockManager = new ReconciliationLockManager();

        // Create a thread pool for the reconciliation loops and add the reconciliation loops
        this.threadPool = new ArrayList<>(config.getControllerThreadPoolSize());
        for (int i = 0; i < config.getControllerThreadPoolSize(); i++)  {
            threadPool.add(new UserControllerLoop(RESOURCE_KIND + "-ControllerLoop-" + i, workQueue, lockManager, scheduledExecutor, client, userLister, secretLister, userOperator, metrics, config));
        }
    }

    /**
     * Enqueues a user based on an event from the KafkaUser informer
     *
     * @param user    User which triggered the event
     * @param action  Type of the event
     */

    private void enqueueKafkaUser(KafkaUser user, String action) {
        LOGGER.infoOp("{} {} in namespace {} was {}", RESOURCE_KIND, user.getMetadata().getName(), user.getMetadata().getNamespace(), action);
        workQueue.enqueue(new SimplifiedReconciliation(RESOURCE_KIND, user.getMetadata().getNamespace(), user.getMetadata().getName()));
    }

    /**
     * Enqueues a user based on an event from the Secrets informer
     *
     * @param userSecret    Secret which triggered the event
     * @param action        Type of the event
     */
    private void enqueueUserSecret(Secret userSecret, String action) {
        LOGGER.infoOp("Secret {} in namespace {} was {}", userSecret.getMetadata().getName(), userSecret.getMetadata().getNamespace(), action);

        // When no secret prefix is configured, we reconcile for every secret
        // When prefix is configured and the Secret starts with it, we remove the prefix and use it as username
        // When prefix is configured and the Secret name does not start with it, we ignore it
        if (secretPrefix == null || secretPrefix.isEmpty()) {
            workQueue.enqueue(new SimplifiedReconciliation(RESOURCE_KIND, userSecret.getMetadata().getNamespace(), userSecret.getMetadata().getName()));
        } else if (userSecret.getMetadata().getName().startsWith(secretPrefix)) {
            String kafkaUser = userSecret.getMetadata().getName().substring(secretPrefix.length());
            workQueue.enqueue(new SimplifiedReconciliation(RESOURCE_KIND, userSecret.getMetadata().getNamespace(), kafkaUser));
        }
    }

    /**
     * Indicates that the informers have been synced and are up-to-date.
     *
     * @return  True when all informers are synced. False otherwise.
     */
    protected boolean isSynced() {
        return secretInformer.hasSynced() && userInformer.hasSynced();
    }

    /**
     * Stops the controller and all its controller loop threads
     */
    protected void stop() {
        LOGGER.infoOp("Stopping scheduled executor service");
        scheduledExecutor.shutdownNow(); // We do not wait for termination

        LOGGER.infoOp("Stopping informers");
        secretInformer.stop();
        userInformer.stop();

        LOGGER.infoOp("Stopping User Controller loops");
        threadPool.forEach(t -> {
            try {
                t.stop();
            } catch (InterruptedException e) {
                LOGGER.debugOp("Interrupted while stopping controller loop", e);
            }
        });
    }

    /**
     * Starts the controllers: its informers, its loop threads etc.
     */
    protected void start() {
        // Configure the event handler for the KafkaUser resources
        this.userInformer.addEventHandlerWithResyncPeriod(new ResourceEventHandler<>() {
            @Override
            public void onAdd(KafkaUser user) {
                metrics.resourceCounter(watchedNamespace).incrementAndGet(); // increases the resource counter

                if (Annotations.isReconciliationPausedWithAnnotation(user)) {
                    // New paused user is added => increase the paused resources counter
                    metrics.pausedResourceCounter(user.getMetadata().getNamespace()).incrementAndGet();
                }

                enqueueKafkaUser(user, "ADDED");
            }

            @Override
            public void onUpdate(KafkaUser oldUser, KafkaUser newUser) {
                if (Annotations.isReconciliationPausedWithAnnotation(oldUser) && !Annotations.isReconciliationPausedWithAnnotation(newUser)) {
                    // User is unpaused => decrement the counter
                    metrics.pausedResourceCounter(watchedNamespace).decrementAndGet();
                } else if (!Annotations.isReconciliationPausedWithAnnotation(oldUser) && Annotations.isReconciliationPausedWithAnnotation(newUser)) {
                    // User is paused => increment the counter
                    metrics.pausedResourceCounter(watchedNamespace).incrementAndGet();
                }

                enqueueKafkaUser(newUser, "MODIFIED");
            }

            @Override
            public void onDelete(KafkaUser user, boolean deletedFinalStateUnknown) {
                metrics.resourceCounter(user.getMetadata().getNamespace()).decrementAndGet(); // decreases the resource counter

                if (Annotations.isReconciliationPausedWithAnnotation(user)) {
                    // Paused user is deleted => decrease the paused resources counter
                    metrics.pausedResourceCounter(watchedNamespace).decrementAndGet();
                }

                enqueueKafkaUser(user, "DELETED");
            }
        }, DEFAULT_RESYNC_PERIOD);

        // Configure the event handler for Secrets
        this.secretInformer.addEventHandlerWithResyncPeriod(new ResourceEventHandler<>() {
            @Override
            public void onAdd(Secret secret) {
                enqueueUserSecret(secret, "ADDED");
            }

            @Override
            public void onUpdate(Secret oldSecret, Secret newSecret) {
                enqueueUserSecret(newSecret, "MODIFIED");
            }

            @Override
            public void onDelete(Secret secret, boolean deletedFinalStateUnknown) {
                enqueueUserSecret(secret, "DELETED");
            }
        }, DEFAULT_RESYNC_PERIOD);

        while (!isSynced())   {
            LOGGER.infoOp("Waiting for the informers to sync");
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                LOGGER.debugOp("Interrupted while waiting for informers to sync", e);
            }
        }

        // Start the controller loop threads
        LOGGER.infoOp("Starting User Controller loops");
        threadPool.forEach(AbstractControllerLoop::start);

        // Configure the periodic reconciliation
        schedulePeriodicReconciliations();
    }

    /**
     * Indicates whether the controller is ready or not. It is considered ready, when all controllers are running.
     *
     * @return  True when the controller is ready, false otherwise
     */
    public boolean isReady()    {
        boolean ready = true;

        for (UserControllerLoop t : threadPool) {
            ready &= t.isRunning();
        }

        return ready;
    }

    /**
     * Indicates whether the controller is alive or not. It is considered alive when all controller loop threads are
     * alive.
     *
     * @return  True when the controller thread is alice, false otherwise
     */
    public boolean isAlive()    {
        boolean ready = true;

        for (UserControllerLoop t : threadPool) {
            ready &= t.isAlive();
        }

        return ready;
    }

    /**
     * Schedules the periodic reconciliation triggers
     */
    private void schedulePeriodicReconciliations()  {
        scheduledExecutor.scheduleAtFixedRate(new PeriodicReconciliation(), reconcileIntervalMs, reconcileIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Internal timer tasks which gets the list of all usernames based on the custom resources, ACLs, Quotas or SCRAM
     * credentials and queues them for reconciliation.
     */
    class PeriodicReconciliation implements Runnable  {
        @Override
        public void run() {
            LOGGER.infoOp("Triggering periodic reconciliation of {} resources for namespace {}", RESOURCE_KIND, watchedNamespace);
            metrics.periodicReconciliationsCounter(watchedNamespace).increment();

            CompletionStage<Set<NamespaceAndName>> allUsersFuture = userOperator.getAllUsers(watchedNamespace);

            try {
                Set<NamespaceAndName> allUsers = allUsersFuture.toCompletableFuture().get(operationTimeoutMs, TimeUnit.MILLISECONDS);
                allUsers.forEach(user -> workQueue.enqueue(new SimplifiedReconciliation(RESOURCE_KIND, user.getNamespace(), user.getName(), "timer")));
            } catch (TimeoutException e)    {
                LOGGER.errorOp("Periodic reconciliation of {} resources for namespace {} timed out", RESOURCE_KIND, watchedNamespace, e);
                allUsersFuture.toCompletableFuture().cancel(true);
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.errorOp("Periodic reconciliation of {} resources for namespace {} failed", RESOURCE_KIND, watchedNamespace, e);
            }
        }
    }
}
