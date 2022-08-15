/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.leaderelection;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;

/**
 * LeaderElectionManager class is responsible for the leader election process. It has its own long-running thread to
 * handle the servicing of the Kubernetes Lease lock. It waits for it until it get available and when it becomes the
 * leader, it also periodically updates the lock to make sure it stays the leader as well. It offers callbacks which
 * the operators can use to integrate with it.
 */
public class LeaderElectionManager implements Runnable  {
    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionManager.class);

    private final KubernetesClient client;
    private final LeaderElectionManagerConfig config;
    private final Runnable startLeadershipCallback;
    private final Runnable stopLeadershipCallback;
    private final Consumer<String> leadershipChangeCallback;
    private final Thread managerThread;

    private boolean isLeader = false;

    /**
     * LeaderElectionManager constructor
     *
     * @param client                    Kubernetes client
     * @param config                    LeaderElectionManager configuration
     * @param startLeadershipCallback   Callback which is called when this instance becomes a leader
     * @param stopLeadershipCallback    Callback which is called when this instance stops being a leader
     * @param leadershipChangeCallback  Callback which is called when the leadership changes and a new leader is elected
     */
    public LeaderElectionManager(KubernetesClient client, LeaderElectionManagerConfig config, Runnable startLeadershipCallback, Runnable stopLeadershipCallback, Consumer<String> leadershipChangeCallback) {
        this.client = client;
        this.config = config;
        this.startLeadershipCallback = startLeadershipCallback;
        this.stopLeadershipCallback = stopLeadershipCallback;
        this.leadershipChangeCallback = leadershipChangeCallback;

        this.managerThread = new Thread(this);
    }

    /**
     * The run() method which just calls into the Fabric8 Kubernetes client API.
     */
    @Override
    public void run() {
        client.leaderElector()
                .withConfig(
                        new LeaderElectionConfigBuilder()
                                .withName(config.getLeaseName())
                                .withLock(new LeaseLock(config.getNamespace(), config.getLeaseName(), config.getIdentity()))
                                .withLeaseDuration(config.getLeaseDuration())
                                .withRenewDeadline(config.getRenewDeadline())
                                .withRetryPeriod(config.getRetryPeriod())
                                .withLeaderCallbacks(new LeaderCallbacks(
                                        () -> startLeadership(),
                                        () -> stopLeadership(),
                                        newLeader -> leadershipChange(newLeader)))
                                .withReleaseOnCancel(true)
                                .build())
                .build().run();

        // Existing the leader election thread => if we are leader, we will loose the leadership
        if (isLeader)   {
            stopLeadership();
        }
    }

    /**
     * Start method to start the election thread
     */
    public void start() {
        managerThread.start();
    }

    /**
     * Stop method to stop the election thread
     */
    public void stop()  {
        managerThread.interrupt();
    }

    /**
     * Internal method which logs that this instance is the leader now calls the configured callback. This is called
     * from the leaderElector.
     */
    private void startLeadership()  {
        isLeader = true;
        LOGGER.info("Started being a leader");
        startLeadershipCallback.run();
    }

    /**
     * Internal method which logs that this instance is NOT the leader anymore and calls the configured callback. This
     * is called from the leaderElector.
     */
    private void stopLeadership()   {
        isLeader = false;
        LOGGER.info("Stopped being a leader");
        stopLeadershipCallback.run();
    }

    /**
     * Internal method which logs that the leadership changed and calls the configured callback. This is called
     * from the leaderElector.
     */
    private void leadershipChange(String newLeader) {
        LOGGER.info("The new leader is {}", newLeader);
        leadershipChangeCallback.accept(newLeader);
    }
}
