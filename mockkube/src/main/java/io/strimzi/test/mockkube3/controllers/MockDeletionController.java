/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3.controllers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The MockDeletionController handles deletion of various resources by removing the finalizers otherwise removed by
 * other Kubernetes components. It currently handles:
 *   - Secrets
 *   - Config Maps
 *   - PVCs
 *   - Services
 *   - Pods
 *   - StrimziPodSet
 *   - NetworkPolicy
 *   - ServiceAccount
 *   - PodDisruptionBudget
 */
public class MockDeletionController extends AbstractMockController {
    private static final Logger LOGGER = LogManager.getLogger(MockDeletionController.class);
    private static final Set<String> FINALIZERS = Set.of("foregroundDeletion", "orphan", "kubernetes.io/pvc-protection");

    private final List<Watch> watches = new ArrayList<>();

    /**
     * Constructs the Mock Deletion Controller
     */
    public MockDeletionController() {
        super();
    }

    /**
     * Starts the watch for handling of deletion
     */
    @Override
    @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"}) // Just a test util, no need to complicate the code bay factoring the anonymous watcher class out
    public void start(KubernetesClient client) {
        watches.add(client.services().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Service service) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.services(), service);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock Service deletion watch closed", e);
            }
        }));

        watches.add(client.secrets().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Secret secret) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.secrets(), secret);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock Secret deletion watch closed", e);
            }
        }));

        watches.add(client.configMaps().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, ConfigMap cm) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.configMaps(), cm);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock ConfigMap deletion watch closed", e);
            }
        }));

        watches.add(client.pods().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Pod pod) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.pods(), pod);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock Pod deletion watch closed", e);
            }
        }));

        watches.add(client.persistentVolumeClaims().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, PersistentVolumeClaim pvc) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.persistentVolumeClaims(), pvc);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock PVC deletion watch closed", e);
            }
        }));

        try {
            watches.add(Crds.strimziPodSetOperation(client).inAnyNamespace().watch(new Watcher<>() {
                @Override
                public void eventReceived(Action action, StrimziPodSet sps) {
                    if (action == Action.MODIFIED) {
                        removeFinalizersIfNeeded(Crds.strimziPodSetOperation(client), sps);
                    }
                }

                @Override
                public void onClose(WatcherException e) {
                    LOGGER.error("Mock StrimziPodSet deletion watch closed", e);
                }
            }));
        } catch (KubernetesClientException e)   {
            if (404 != e.getCode()) {
                // 404 error might happen if the StrimziPodSet CRD is missing. In that cae, we just ignore the exception. Otherwise. we rethrow it.
                throw e;
            }
        }

        watches.add(client.network().networkPolicies().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, NetworkPolicy networkPolicy) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.network().networkPolicies(), networkPolicy);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock NetworkPolicy deletion watch closed", e);
            }
        }));

        watches.add(client.serviceAccounts().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, ServiceAccount serviceAccount) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.serviceAccounts(), serviceAccount);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock ServiceAccount deletion watch closed", e);
            }
        }));

        watches.add(client.policy().v1().podDisruptionBudget().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, PodDisruptionBudget podDisruptionBudget) {
                if (action == Action.MODIFIED) {
                    removeFinalizersIfNeeded(client.policy().v1().podDisruptionBudget(), podDisruptionBudget);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock PodDisruptionBudget deletion watch closed", e);
            }
        }));
    }

    private <T extends HasMetadata, R extends Resource<T>, L> void removeFinalizersIfNeeded(MixedOperation<T, L, R> op, T resource)    {
        if (resource.getMetadata().getDeletionGracePeriodSeconds() != null
                && !Collections.disjoint(resource.getMetadata().getFinalizers(), FINALIZERS)) { // Any of the finalizers we watch for is set
            try {
                LOGGER.info("{} {} in namespace {} is terminating with finalizers => finalizer will be removed", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                op.inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).edit(r -> {
                    r.getMetadata().getFinalizers().removeAll(FINALIZERS);
                    return r;
                });
            } catch (KubernetesClientException e)   {
                if (e.getCode() == 409) {
                    LOGGER.info("{} {} in namespace {} changed while trying to update status", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                } else if (e.getCode() == 404) {
                    LOGGER.info("{} {} in namespace {} does not exist anymore", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace());
                } else {
                    LOGGER.error("Failed to update status of {} {} in namespace {}", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), e);
                }
            }
        }
    }

    /**
     * Stops the watches for the various resources
     */
    @Override
    public void stop() {
        for (Watch watch : watches) {
            watch.close();
        }
    }
}
