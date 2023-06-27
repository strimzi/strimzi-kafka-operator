/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class NodeUtils {

    private static final Logger LOGGER = LogManager.getLogger(NodeUtils.class);

    public static Map<String, List<String>> getPodsForEachNodeInNamespace(String namespaceName) {
        List<Node> workerNodes = kubeClient().listWorkerNodes();
        List<Pod> pods = kubeClient().listPods(namespaceName);

        Map<String, List<String>> podsInNodes = new HashMap<>();

        workerNodes
            .forEach(node -> podsInNodes.put(
                node.getMetadata().getName(),
                pods.stream().filter(pod -> pod.getSpec().getNodeName().equals(node.getMetadata().getName()))
                    .map(pod -> pod.getMetadata().getName())
                    .collect(Collectors.toList()))
        );

        return podsInNodes;
    }

    public static void drainNode(String nodeName) {
        List<String> cmd = new ArrayList<>();

        if (KubeClusterResource.getInstance().isOpenShift()) {
            cmd.add("adm");
        }

        cmd.addAll(Arrays.asList("drain", nodeName, "--delete-local-data", "--force", "--ignore-daemonsets"));

        LOGGER.info("Draining cluster node: {}", nodeName);
        cordonNode(nodeName, false);

        KubeClusterResource.cmdKubeClient().exec(cmd);
    }

    public static void cordonNode(String node, boolean schedule) {
        LOGGER.info("Setting {} schedule {}", node, schedule);
        List<String> cmd = new ArrayList<>();

        if (KubeClusterResource.getInstance().isOpenShift()) {
            cmd.add("adm");
        }

        cmd.addAll(Arrays.asList(schedule ? "uncordon" : "cordon", node));

        KubeClusterResource.cmdKubeClient().exec(cmd);
    }
}
