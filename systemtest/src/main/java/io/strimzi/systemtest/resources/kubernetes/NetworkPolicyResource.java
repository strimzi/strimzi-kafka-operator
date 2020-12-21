package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.DoneableNetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NetworkPolicyResource implements ResourceType<NetworkPolicy> {

    private static final Logger LOGGER = LogManager.getLogger(NetworkPolicyResource.class);

    @Override
    public String getKind() {
        return "NetworkPolicy";
    }
    @Override
    public NetworkPolicy get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getNetworkPolicy(name);
    }
    @Override
    public void create(NetworkPolicy resource) {
        ResourceManager.kubeClient().createNetworkPolicy(resource);
    }
    @Override
    public void delete(NetworkPolicy resource) throws Exception {
        ResourceManager.kubeClient().deleteNetworkPolicy(resource.getMetadata().getName());
    }
    @Override
    public boolean isReady(NetworkPolicy resource) {
        return resource != null;
    }
    @Override
    public void refreshResource(NetworkPolicy existing, NetworkPolicy newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
    }

    /**
     * Method for allowing network policies for Connect or ConnectS2I
     * @param resource mean Connect or ConnectS2I resource
     * @param deploymentName name of resource deployment - for setting strimzi.io/name
     */
    public static void allowNetworkPolicySettingsForResource(ExtensionContext extensionContext, HasMetadata resource, String deploymentName) {
        LabelSelector labelSelector = new LabelSelectorBuilder()
            .addToMatchLabels(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE)
            .build();

        if (kubeClient().listPods(labelSelector).size() == 0) {
            throw new RuntimeException("You did not create the Kafka Client instance(pod) before using the " + resource.getKind());
        }

        LOGGER.info("Apply NetworkPolicy access to {} from pods with LabelSelector {}", deploymentName, labelSelector);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withNewApiVersion("networking.k8s.io/v1")
            .withNewKind("NetworkPolicy")
            .withNewMetadata()
            .withName(resource.getMetadata().getName() + "-allow")
            .endMetadata()
            .withNewSpec()
            .addNewIngress()
            .addNewFrom()
            .withPodSelector(labelSelector)
            .endFrom()
            .addNewPort()
            .withNewPort(8083)
            .withNewProtocol("TCP")
            .endPort()
            .addNewPort()
            .withNewPort(9404)
            .withNewProtocol("TCP")
            .endPort()
            .addNewPort()
            .withNewPort(8080)
            .withNewProtocol("TCP")
            .endPort()
            .endIngress()
            .withNewPodSelector()
            .addToMatchLabels("strimzi.io/cluster", resource.getMetadata().getName())
            .addToMatchLabels("strimzi.io/kind", resource.getKind())
            .addToMatchLabels("strimzi.io/name", deploymentName)
            .endPodSelector()
            .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for LabelSelector {} successfully applied", labelSelector);
    }

    public static NetworkPolicy applyDefaultNetworkPolicy(ExtensionContext extensionContext, String namespace, DefaultNetworkPolicy policy) {
        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withNewApiVersion("networking.k8s.io/v1")
            .withNewKind("NetworkPolicy")
            .withNewMetadata()
            .withName("global-network-policy")
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withNewPodSelector()
            .endPodSelector()
            .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        if (policy.equals(DefaultNetworkPolicy.DEFAULT_TO_ALLOW)) {
            networkPolicy = new NetworkPolicyBuilder(networkPolicy)
                .editSpec()
                .addNewIngress()
                .endIngress()
                .endSpec()
                .build();
        }

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());

        return networkPolicy;
    }
}
