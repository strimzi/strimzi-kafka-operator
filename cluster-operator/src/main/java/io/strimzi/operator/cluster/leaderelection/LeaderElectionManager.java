/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * LeaderElectionManager class is responsible for the leader election process. It is based on a future provided by the
 * Fabric8 leader elector. It waits for it until it get available and when it becomes the leader, it also periodically
 * updates the lock to make sure it stays the leader as well. It offers callbacks which the operators can use to
 * integrate with it.
 */
public class LeaderElectionManager {
    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionManager.class);

    private final Runnable startLeadershipCallback;
    private final Consumer<Boolean> stopLeadershipCallback;
    private final Consumer<String> leadershipChangeCallback;
    private final LeaderElector leaderElector;

    private CompletableFuture<?> leaderElectorFuture;
    private boolean isShuttingDown = false;

    /**
     * LeaderElectionManager constructor
     *
     * @param client                    Kubernetes client
     * @param config                    LeaderElectionManager configuration
     * @param startLeadershipCallback   Callback which is called when this instance becomes a leader
     * @param stopLeadershipCallback    Callback which is called when this instance stops being a leader
     * @param leadershipChangeCallback  Callback which is called when the leadership changes and a new leader is elected
     */
    public LeaderElectionManager(KubernetesClient client, LeaderElectionManagerConfig config, Runnable startLeadershipCallback, Consumer<Boolean> stopLeadershipCallback, Consumer<String> leadershipChangeCallback) {
        this.startLeadershipCallback = startLeadershipCallback;
        this.stopLeadershipCallback = stopLeadershipCallback;
        this.leadershipChangeCallback = leadershipChangeCallback;
        this.leaderElector = client.leaderElector()
                .withConfig(new LeaderElectionConfigBuilder()
                        .withReleaseOnCancel()
                        .withName(config.getLeaseName())
                        .withLock(new LeaseLock(config.getNamespace(), config.getLeaseName(), config.getIdentity()))
                        .withLeaseDuration(config.getLeaseDuration())
                        .withRenewDeadline(config.getRenewDeadline())
                        .withRetryPeriod(config.getRetryPeriod())
                        .withLeaderCallbacks(new LeaderCallbacks(
                                this::startLeadershipHandler,
                                this::stopLeadershipHandler,
                                this::leadershipChangeHandler))
                        .build())
                .build();
    }

    /**
     * Starts the Leader Elector
     */
    public void start() {
        LOGGER.info("Starting the Leader Elector");
        leaderElectorFuture = leaderElector.start();
    }

    /**
     * Stop method to stop the Leader Elector
     */
    public void stop()  {
        if (leaderElectorFuture == null)    {
            LOGGER.info("Leader Elector was not started yet");
        } else if (leaderElectorFuture.isDone())   {
            LOGGER.info("Leader Elector is already stopped");
        } else {
            LOGGER.info("Stopping the Leader Elector");
            isShuttingDown = true;
            leaderElectorFuture.cancel(true);

            LOGGER.info("Waiting for Leader Elector to stop");

            try {
                leaderElectorFuture.join();
            } catch (CancellationException e)   {
                // Nothing to do, we just canceled it
            }

            LOGGER.info("Leader Elector stopped");
        }
    }

    /**
     * Internal method which logs that this instance is the leader now calls the configured callback. This is called
     * from the leaderElector.
     */
    private void startLeadershipHandler()  {
        LOGGER.info("Started being a leader");
        startLeadershipCallback.run();
    }

    /**
     * Internal method which logs that this instance is NOT the leader anymore and calls the configured callback. This
     * is called from the leaderElector.
     */
    private void stopLeadershipHandler()   {
        LOGGER.info("Stopped being a leader");
        stopLeadershipCallback.accept(isShuttingDown);
    }

    /**
     * Internal method which logs that the leadership changed and calls the configured callback. This is called
     * from the leaderElector.
     */
    private void leadershipChangeHandler(String newLeader) {
        LOGGER.info("The new leader is {}", newLeader);
        leadershipChangeCallback.accept(newLeader);
    }
}
