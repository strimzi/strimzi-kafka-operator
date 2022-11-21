/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube2.controllers;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MockStatefulSetController partially emulates the Kubernetes StatefulSet controller. When new StatefulSet is created
 * or modified, it updates its status to mark it as ready. It also handles creation, updates and deletion of Pods when
 * the StatefulSet is deleted or when it scales up or down.
 */
public class MockStatefulSetController extends AbstractMockController {
    private static final Logger LOGGER = LogManager.getLogger(MockStatefulSetController.class);

    private Watch watch;
    private Watch podWatch;

    /**
     * Constructs the Mock StatefulSet controller
     *
     * @param client    Kubernetes client
     */
    public MockStatefulSetController(KubernetesClient client) {
        super(client);
    }

    /**
     * Starts the watch for new or updated StatefulSets and Pods
     */
    @Override
    public void start() {
        watch = client.apps().statefulSets().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Watcher.Action action, StatefulSet sts) {
                switch (action)  {
                    case ADDED:
                    case MODIFIED:
                        String name = sts.getMetadata().getName();
                        String namespace = sts.getMetadata().getNamespace();

                        try {
                            // Create Pods
                            for (int i = 0; i < sts.getSpec().getReplicas(); i++)   {
                                String podName = name + "-" + i;
                                createPodIfNeeded(sts, namespace, podName);
                            }

                            // Delete pods (when scaling down)
                            client.pods().inNamespace(namespace)
                                    .withLabelSelector(sts.getSpec().getSelector())
                                    .list()
                                    .getItems().forEach(pod -> {
                                        int podIndex = Integer.parseInt(pod.getMetadata().getName().substring(pod.getMetadata().getName().lastIndexOf("-") + 1));
                                        if (podIndex >= sts.getSpec().getReplicas())    {
                                            LOGGER.info("Deleting scaled down pod {}", pod.getMetadata().getName());
                                            client.pods().inNamespace(namespace).withName(pod.getMetadata().getName()).delete();
                                        }
                                    });

                            // Set StatefulSet status
                            client.apps().statefulSets().inNamespace(namespace).resource(new StatefulSetBuilder(sts)
                                    .withStatus(new StatefulSetStatusBuilder()
                                            .withObservedGeneration(sts.getMetadata().getGeneration())
                                            .withCurrentReplicas(sts.getSpec().getReplicas())
                                            .withReplicas(sts.getSpec().getReplicas())
                                            .withReadyReplicas(sts.getSpec().getReplicas())
                                            .build())
                                    .build())
                                    .replaceStatus();
                        } catch (KubernetesClientException e)   {
                            if (e.getCode() == 409) {
                                LOGGER.info("StatefulSet {} in namespace {} changed while trying to update status", name, namespace);
                            } else if (e.getCode() == 404) {
                                LOGGER.info("StatefulSet {} in namespace {} does not exist anymore", name, namespace);
                            } else {
                                LOGGER.error("Failed to update status of StatefulSet {} in namespace {}", name, namespace, e);
                            }
                        }

                        break;
                    default:
                        // Nothing to do
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock StatefulSet controller watch closed", e);
            }
        });

        podWatch = client.pods().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Watcher.Action action, Pod pod) {
                switch (action)  {
                    case DELETED:
                        if (pod.getMetadata() != null
                                && pod.getMetadata().getOwnerReferences() != null)  {
                            OwnerReference owner = pod.getMetadata().getOwnerReferences().stream().filter(or -> "StatefulSet".equals(or.getKind())).findFirst().orElse(null);

                            if (owner != null)  {
                                StatefulSet sts = client.apps().statefulSets().inNamespace(pod.getMetadata().getNamespace()).withName(owner.getName()).get();

                                if (sts != null)    {
                                    int podIndex = Integer.parseInt(pod.getMetadata().getName().substring(pod.getMetadata().getName().lastIndexOf("-") + 1));

                                    if (podIndex < sts.getSpec().getReplicas()) {
                                        LOGGER.info("Recreating restarted pod");
                                        createPodIfNeeded(sts, pod.getMetadata().getNamespace(), pod.getMetadata().getName());
                                    }
                                }
                            }
                        }

                        break;
                    default:
                        // Nothing to do
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Pod watch in StatefulSet controller closed", e);
            }
        });
    }

    private void createPodIfNeeded(StatefulSet sts, String namespace, String name) {
        try {
            Pod pod = client.pods().inNamespace(namespace).withName(name).get();

            if (pod == null) {
                client.pods().inNamespace(namespace).resource(new PodBuilder()
                        .withNewMetadataLike(sts.getSpec().getTemplate().getMetadata())
                            .withNamespace(namespace)
                            .withName(name)
                            .addNewOwnerReference()
                                .withKind(sts.getKind())
                                .withName(sts.getMetadata().getName())
                            .endOwnerReference()
                        .endMetadata()
                        .withNewSpecLike(sts.getSpec().getTemplate().getSpec()).endSpec()
                        .build())
                        .createOrReplace();
            }
        } catch (KubernetesClientException e) {
            LOGGER.error("Failed to recreate Pod {} in namespace {}", name, namespace, e);
        }
    }

    /**
     * Stops the watch for StatefulSet and Pod resources
     */
    @Override
    public void stop() {
        watch.close();
        podWatch.close();
    }
}
