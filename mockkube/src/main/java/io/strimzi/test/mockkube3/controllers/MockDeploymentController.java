/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3.controllers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MockDeploymentController partially emulates the Kubernetes Deployment controller. When new Deployment is created
 * or modified, it updates its status to mark it as ready. It does not create any actual pods.
 */
public class MockDeploymentController extends AbstractMockController {
    private static final Logger LOGGER = LogManager.getLogger(MockDeploymentController.class);

    private Watch watch;

    /**
     * Constructs the Mock Deployment controller
     */
    public MockDeploymentController() {
        super();
    }

    /**
     * Starts the watch for new or updated Deployments
     */
    @Override
    @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"}) // Just a test util, no need to complicate the code bay factoring the anonymous watcher class out
    public void start(KubernetesClient client) {
        watch = client.apps().deployments().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Deployment deployment) {
                switch (action)  {
                    case ADDED:
                    case MODIFIED:
                        try {
                            client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).resource(new DeploymentBuilder(deployment)
                                            .withStatus(new DeploymentStatusBuilder()
                                                    .withObservedGeneration(deployment.getMetadata().getGeneration())
                                                    .withReplicas(deployment.getSpec().getReplicas())
                                                    .withAvailableReplicas(deployment.getSpec().getReplicas())
                                                    .withReadyReplicas(deployment.getSpec().getReplicas())
                                                    .build())
                                            .build())
                                    .updateStatus();
                        } catch (KubernetesClientException e)   {
                            if (e.getCode() == 409) {
                                LOGGER.info("Deployment {} in namespace {} changed while trying to update status", deployment.getMetadata().getName(), deployment.getMetadata().getNamespace());
                            } else if (e.getCode() == 404) {
                                LOGGER.info("Deployment {} in namespace {} does not exist anymore", deployment.getMetadata().getName(), deployment.getMetadata().getNamespace());
                            } else {
                                LOGGER.error("Failed to update status of Deployment {} in namespace {}", deployment.getMetadata().getName(), deployment.getMetadata().getNamespace(), e);
                            }
                        }

                        break;
                    default:
                        // Nothing to do
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock Deployment controller watch closed", e);
            }
        });
    }

    /**
     * Stops the watch for Deployment resources
     */
    @Override
    public void stop() {
        watch.close();
    }
}
