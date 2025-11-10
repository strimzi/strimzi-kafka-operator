/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3.controllers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The {@link MockPvcController} partially emulates a CSI driver. When new PVC is created
 * or modified, it updates its phase to Bound. It doesn't create or bind any {@code PersistentVolumes}.
 */
public class MockPvcController extends AbstractMockController {
    private static final Logger LOGGER = LogManager.getLogger(MockPvcController.class);

    private Watch watch;

    @Override
    @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"}) // Just a test util, no need to complicate the code bay factoring the anonymous watcher class out
    public void start(KubernetesClient client) {
        watch = client.persistentVolumeClaims().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, PersistentVolumeClaim pvc) {
                try {
                    switch (action) {
                        case ADDED:
                        case MODIFIED:
                            client.persistentVolumeClaims().resource(new PersistentVolumeClaimBuilder(pvc)
                                    .withStatus(new PersistentVolumeClaimStatusBuilder()
                                        .withPhase("Bound")
                                        .build())
                                    .build())
                                .updateStatus();
                            break;
                        default:
                    }
                } catch (KubernetesClientException e) {
                    if (e.getCode() == 409) {
                        LOGGER.info("PVC {} in namespace {} changed while trying to update status", pvc.getMetadata().getName(), pvc.getMetadata().getNamespace());
                    } else if (e.getCode() == 404) {
                        LOGGER.info("PVC {} in namespace {} does not exist anymore", pvc.getMetadata().getName(), pvc.getMetadata().getNamespace());
                    } else {
                        LOGGER.error("Failed to update status of PVC {} in namespace {}", pvc.getMetadata().getName(), pvc.getMetadata().getNamespace(), e);
                    }
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock PVC controller watch closed", e);
            }
        });
    }

    @Override
    public void stop() {
        watch.close();
    }
}
