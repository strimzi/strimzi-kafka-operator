/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.Service;

import static io.strimzi.test.BaseITST.kubeClient;

public class BridgeUtils {

    public static int getBridgeNodePort(String namespace, String bridgeExternalService) {
        Service extBootstrapService = kubeClient(namespace).getClient().services()
                .inNamespace(namespace)
                .withName(bridgeExternalService)
                .get();

        return extBootstrapService.getSpec().getPorts().get(0).getNodePort();
    }
}
