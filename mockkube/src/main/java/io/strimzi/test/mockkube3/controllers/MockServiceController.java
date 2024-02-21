/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3.controllers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.EndpointAddressBuilder;
import io.fabric8.kubernetes.api.model.EndpointPortBuilder;
import io.fabric8.kubernetes.api.model.EndpointSubsetBuilder;
import io.fabric8.kubernetes.api.model.EndpointsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MockServiceController partially emulates the Kubernetes Service controller. When new Service is created
 * or modified, it creates the corresponding ServiceEndpoints resource.
 */
public class MockServiceController extends AbstractMockController {
    private static final Logger LOGGER = LogManager.getLogger(MockServiceController.class);

    private Watch watch;

    /**
     * Constructs the Mock Service controller
     */
    public MockServiceController() {
        super();
    }

    /**
     * Starts the watch for new or updated Services
     */
    @Override
    @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC_ANON"}) // Just a test util, no need to complicate the code bay factoring the anonymous watcher class out
    public void start(KubernetesClient client) {
        watch = client.services().inAnyNamespace().watch(new Watcher<>() {
            @Override
            public void eventReceived(Watcher.Action action, Service svc) {
                // Ignore default Kubernetes service
                if ("kubernetes".equals(svc.getMetadata().getName()) && "default".equals(svc.getMetadata().getNamespace())) {
                    return;
                }

                switch (action)  {
                    case ADDED:
                    case MODIFIED:
                        String name = svc.getMetadata().getName();
                        String namespace = svc.getMetadata().getNamespace();

                        try {
                            if (client.endpoints().inNamespace(namespace).withName(name).get() != null) {
                                client.endpoints().inNamespace(namespace).resource(new EndpointsBuilder()
                                                .withNewMetadata()
                                                .withName(name)
                                                .withNamespace(namespace)
                                                .endMetadata()
                                                .withSubsets(new EndpointSubsetBuilder()
                                                        .withAddresses(new EndpointAddressBuilder().withHostname("some-address").withIp("10.9.8.7").build())
                                                        .withPorts(new EndpointPortBuilder().withPort(1234).build())
                                                        .build())
                                                .build())
                                        .update();
                            } else {
                                client.endpoints().inNamespace(namespace).resource(new EndpointsBuilder()
                                                .withNewMetadata()
                                                .withName(name)
                                                .withNamespace(namespace)
                                                .endMetadata()
                                                .withSubsets(new EndpointSubsetBuilder()
                                                        .withAddresses(new EndpointAddressBuilder().withHostname("some-address").withIp("10.9.8.7").build())
                                                        .withPorts(new EndpointPortBuilder().withPort(1234).build())
                                                        .build())
                                                .build())
                                        .create();
                            }
                        } catch (KubernetesClientException e)   {
                            LOGGER.error("Failed to update Endpoint {} in namespace {}", name, namespace, e);
                        }

                        break;
                    default:
                        // Nothing to do
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LOGGER.error("Mock Service controller watch closed", e);
            }
        });
    }

    /**
     * Stops the watch for Services resources
     */
    @Override
    public void stop() {
        watch.close();
    }
}
