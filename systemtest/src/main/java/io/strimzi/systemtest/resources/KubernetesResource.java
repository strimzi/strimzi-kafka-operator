/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KubernetesResource {
    private static final Logger LOGGER = LogManager.getLogger(KubernetesResource.class);

    public static DoneableDeployment deployNewDeployment(Deployment deployment) {
        return new DoneableDeployment(deployment, co -> {
            TestUtils.waitFor("Deployment creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
                () -> {
                    try {
                        ResourceManager.kubeClient().createOrReplaceDeployment(co);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(co));
        });
    }

    public static DoneableJob deployNewJob(Job job) {
        return new DoneableJob(job, kubernetesJob -> {
            TestUtils.waitFor("Job creation " + job.getMetadata().getName(), Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
                () -> {
                    try {
                        ResourceManager.kubeClient().getClient().batch().jobs().inNamespace(kubeClient().getNamespace()).createOrReplace(kubernetesJob);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return deleteLater(kubernetesJob);
        });
    }

    public static DoneableRoleBinding roleBinding(String yamlPath, String namespace, String clientNamespace) {
        LOGGER.info("Creating RoleBinding from {} in namespace {}", yamlPath, namespace);
        RoleBinding roleBinding = getRoleBindingFromYaml(yamlPath);
        return roleBinding(new RoleBindingBuilder(roleBinding)
            .editFirstSubject()
                .withNamespace(namespace)
            .endSubject().build(), clientNamespace);
    }

    private static DoneableRoleBinding roleBinding(RoleBinding roleBinding, String clientNamespace) {
        ResourceManager.kubeClient().namespace(clientNamespace).createOrReplaceRoleBinding(roleBinding);
        deleteLater(roleBinding);
        return new DoneableRoleBinding(roleBinding);
    }

    public static DoneableClusterRoleBinding clusterRoleBinding(String yamlPath, String namespace, String clientNamespace) {
        LOGGER.info("Creating ClusterRoleBinding from {} in namespace {}", yamlPath, namespace);
        ClusterRoleBinding clusterRoleBinding = getClusterRoleBindingFromYaml(yamlPath);
        return clusterRoleBinding(new ClusterRoleBindingBuilder(clusterRoleBinding)
            .editFirstSubject()
                .withNamespace(namespace)
            .endSubject().build(), clientNamespace);
    }

    public static DoneableClusterRoleBinding clusterRoleBinding(ClusterRoleBinding clusterRoleBinding, String clientNamespace) {
        ResourceManager.kubeClient().createOrReplaceClusterRoleBinding(clusterRoleBinding);
        deleteLater(clusterRoleBinding);
        return new DoneableClusterRoleBinding(clusterRoleBinding);
    }

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespace) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");

        List<ClusterRoleBinding> kCRBList = new ArrayList<>();

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-cluster-operator-namespaced")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-cluster-operator-namespaced")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-entity-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-entity-operator")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName("strimzi-topic-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-topic-operator")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );
        return kCRBList;
    }

    public static ServiceBuilder getSystemtestsServiceResource(String appName, int port, String namespace, String transportProtocol) {
        return new ServiceBuilder()
            .withNewMetadata()
                .withName(appName)
                .withNamespace(namespace)
                .addToLabels("run", appName)
            .endMetadata()
            .withNewSpec()
                .withSelector(Collections.singletonMap("app", appName))
                .addNewPort()
                    .withName("http")
                    .withPort(port)
                    .withProtocol(transportProtocol)
                .endPort()
            .endSpec();
    }

    public static DoneableService createServiceResource(String appName, int port, String clientNamespace, String transportProtocol) {
        Service service = getSystemtestsServiceResource(appName, port, clientNamespace, transportProtocol).build();
        LOGGER.info("Creating Service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        ResourceManager.kubeClient().createService(service);
        deleteLater(service);
        return new DoneableService(service);
    }

    public static DoneableService createServiceResource(Service service, String clientNamespace) {
        LOGGER.info("Creating Service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        ResourceManager.kubeClient().createService(service);
        deleteLater(service);
        return new DoneableService(service);
    }

    public static Service createKeycloakNodePortHttpService(String namespace) {
        String keycloakName = "keycloak";

        Map<String, String> keycloakLabels = new HashMap<>();
        keycloakLabels.put("app", keycloakName);
        keycloakLabels.put("component", keycloakName);

        return getSystemtestsServiceResource(keycloakName + "-service-http",
            Constants.HTTP_KEYCLOAK_DEFAULT_PORT, namespace, "TCP")
            .editSpec()
                .withType("NodePort")
                .withSelector(keycloakLabels)
                .editFirstPort()
                    .withNodePort(Constants.HTTP_KEYCLOAK_DEFAULT_NODE_PORT)
                .endPort()
            .endSpec().build();
    }

    public static Service createKeycloakNodePortService(String namespace) {
        String keycloakName = "keycloak";

        Map<String, String> keycloakLabels = new HashMap<>();
        keycloakLabels.put("app", keycloakName);
        keycloakLabels.put("component", keycloakName);

        return getSystemtestsServiceResource(keycloakName + "-service-https",
            Constants.HTTPS_KEYCLOAK_DEFAULT_PORT, namespace, "TCP")
            .editSpec()
                .withType("NodePort")
                .withSelector(keycloakLabels)
                .editFirstPort()
                    .withNodePort(Constants.HTTPS_KEYCLOAK_DEFAULT_NODE_PORT)
                .endPort()
            .endSpec().build();
    }

    public static Service deployBridgeNodePortService(String bridgeExternalService, String namespace, String clusterName) {
        Map<String, String> map = new HashMap<>();
        map.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        map.put(Labels.STRIMZI_KIND_LABEL, "KafkaBridge");
        map.put(Labels.STRIMZI_NAME_LABEL, clusterName + "-bridge");

        // Create node port service for expose bridge outside the cluster
        return getSystemtestsServiceResource(bridgeExternalService, Constants.HTTP_BRIDGE_DEFAULT_PORT, namespace, "TCP")
            .editSpec()
                .withType("NodePort")
                .withSelector(map)
            .endSpec().build();
    }

    public static void applyDefaultNetworkPolicySettings(List<String> namespaces) {
        for (String namespace : namespaces) {
            if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES.equals(Boolean.TRUE.toString())) {
                applyDefaultNetworkPolicy(namespace, DefaultNetworkPolicy.DEFAULT_TO_DENY);
            } else {
                applyDefaultNetworkPolicy(namespace, DefaultNetworkPolicy.DEFAULT_TO_ALLOW);
            }
            LOGGER.info("NetworkPolicy successfully set to: {} for namespace: {}", Environment.DEFAULT_TO_DENY_NETWORK_POLICIES, namespace);
        }
    }

    /**
     * Method for allowing network policies for Connect or ConnectS2I
     * @param resource mean Connect or ConnectS2I resource
     * @param deploymentName name of resource deployment - for setting strimzi.io/name
     */
    public static void allowNetworkPolicySettingsForResource(HasMetadata resource, String deploymentName) {
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
        deleteLater(kubeClient().getClient().network().networkPolicies().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(networkPolicy));
        LOGGER.info("Network policy for LabelSelector {} successfully applied", labelSelector);
    }

    public static NetworkPolicy applyDefaultNetworkPolicy(String namespace, DefaultNetworkPolicy policy) {
        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                    .withNewApiVersion("networking.k8s.io/v1")
                    .withNewKind("NetworkPolicy")
                    .withNewMetadata()
                        .withName("global-network-policy")
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
        deleteLater(kubeClient().getClient().network().networkPolicies().inNamespace(namespace).createOrReplace(networkPolicy));
        LOGGER.info("Network policy successfully set to: {}", policy);

        return networkPolicy;
    }

    public static Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }

    private static RoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, RoleBinding.class);
    }

    private static ClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, ClusterRoleBinding.class);
    }

    private static Deployment waitFor(Deployment deployment) {
        String deploymentName = deployment.getMetadata().getName();
        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, deployment.getSpec().getReplicas());
        return deployment;
    }

    private static Deployment deleteLater(Deployment resource) {
        return ResourceManager.deleteLater(ResourceManager.kubeClient().getClient().apps().deployments(), resource);
    }

    private static ClusterRoleBinding deleteLater(ClusterRoleBinding resource) {
        return ResourceManager.deleteLater(ResourceManager.kubeClient().getClient().rbac().clusterRoleBindings(), resource);
    }

    private static RoleBinding deleteLater(RoleBinding resource) {
        return ResourceManager.deleteLater(ResourceManager.kubeClient().getClient().rbac().roleBindings(), resource);
    }

    private static Service deleteLater(Service resource) {
        return ResourceManager.deleteLater(ResourceManager.kubeClient().getClient().services(), resource);
    }

    public static NetworkPolicy deleteLater(NetworkPolicy resource) {
        return ResourceManager.deleteLater(ResourceManager.kubeClient().getClient().network().networkPolicies(), resource);
    }

    public static Job deleteLater(Job resource) {
        return ResourceManager.deleteLater(ResourceManager.kubeClient().getClient().batch().jobs(), resource);
    }
}
