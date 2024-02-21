/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3.controllers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * The MockPodController partially emulates the Kubernetes Pod controller. When new Pod is created
 * or modified, it updates its status to mark it as ready.
 */
public class MockPodController extends AbstractMockController {
    private static final Logger LOGGER = LogManager.getLogger(MockPodController.class);

    private Watch watch;

    /**
     * Annotation to indicate to the Mock Pod controller that this Pod should not be automatically set to ready. This
     * is used to test failure states.
     */
    public static final String ANNO_DO_NOT_SET_READY = "mock-pod-controller/do-not-set-ready";

    /**
     * Constructs the Mock Pod controller
     */
    public MockPodController() {
        super();
    }

    /**
     * Starts the watch for new or updated Pods
     */
    @Override
    @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"}) // Just a test util, no need to complicate the code bay factoring the anonymous watcher class out
    public void start(KubernetesClient client) {
        watch = client.pods().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Watcher.Action action, Pod pod) {
                switch (action)  {
                    case ADDED:
                    case MODIFIED:
                        ObjectMeta podMeta = pod.getMetadata();
                        try {
                            Map<String, String> annotations = podMeta.getAnnotations();

                            if (podMeta.getDeletionGracePeriodSeconds() != null)    {
                                // Nothing to do, Pod is being deleted
                                return;
                            } else if (annotations != null && annotations.containsKey(ANNO_DO_NOT_SET_READY)) {
                                // For some test cases, a pod always being set to Ready isn't desired
                                return;
                            } else {
                                client.pods().inNamespace(pod.getMetadata().getNamespace()).resource(new PodBuilder(pod)
                                                .withStatus(new PodStatusBuilder()
                                                        .withPhase("Running")
                                                        .withConditions(new PodConditionBuilder().withType("Ready").withStatus("True").build())
                                                        .build())
                                                .build())
                                        .updateStatus();
                            }
                        } catch (KubernetesClientException e)   {
                            if (e.getCode() == 409) {
                                LOGGER.info("Pod {} in namespace {} changed while trying to update status", pod.getMetadata().getName(), pod.getMetadata().getNamespace());
                            } else if (e.getCode() == 404) {
                                LOGGER.info("Pod {} in namespace {} does not exist anymore", pod.getMetadata().getName(), pod.getMetadata().getNamespace());
                            } else {
                                LOGGER.error("Failed to update status of Pod {} in namespace {}", pod.getMetadata().getName(), pod.getMetadata().getNamespace(), e);
                            }
                        }

                        break;
                    default:
                        // Nothing to do
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock Pod controller watch closed", e);
            }
        });
    }

    /**
     * Stops the watch for Pod resources
     */
    @Override
    public void stop() {
        watch.close();
    }
}
