/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class TracingUtils {

    private TracingUtils() {}

    public static Service createJaegerHostNodePortService(String clusterName, String namespace, String serviceName) {
        Map<String, String> map = new HashMap<>();
        map.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        map.put(Labels.STRIMZI_KIND_LABEL, "Kafka");
        map.put(Labels.STRIMZI_NAME_LABEL, clusterName + "-tracing");

        // Create node port service for expose bridge outside Kubernetes
        return KubernetesResource.getSystemtestsServiceResource(serviceName, Constants.HTTP_JAEGER_DEFAULT_TCP_PORT, namespace, "TCP")
            .editSpec()
                .withType("NodePort")
                .withSelector(map)
                .editFirstPort()
                    .withNodePort(Constants.HTTP_JAEGER_DEFAULT_NODE_PORT)
                .endPort()
            .endSpec().build();
    }


    public static int getJaegerHostNodePort(String namespace, String tracingExternalService) {
        Service extBootstrapService = kubeClient(namespace).getClient().services()
            .inNamespace(namespace)
            .withName(tracingExternalService)
            .get();

        return extBootstrapService.getSpec().getPorts().get(0).getNodePort();
    }
}
