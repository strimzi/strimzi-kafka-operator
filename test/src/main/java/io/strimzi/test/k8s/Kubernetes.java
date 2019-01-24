package io.strimzi.test.k8s;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.test.TestUtils;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Kubernetes {

    static final long GLOBAL_TIMEOUT = 300000;
    static final long GLOBAL_POLL_INTERVAL = 1000;
    private static final Logger LOGGER = LogManager.getLogger(Kubernetes.class);

    private KubernetesClient client = getInstance();
    private String namespace;

    private static class ClientHolder {
        static final KubernetesClient CLIENT = new DefaultKubernetesClient();
    }

    public static KubernetesClient getInstance() {
        return ClientHolder.CLIENT;
    }

    public String namespace(String namespace) {
        String previous = this.namespace;
        this.namespace = namespace;
        return previous;
    }

    public void createNameSpace(String name) {
        Namespace ns = new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build();
        client.namespaces().createOrReplace(ns);
    }

    public void deleteNamespace(String name) {
        client.namespaces().withName(name).delete();
    }

    public String execInPod(String podName, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CompletableFuture<String> data = new CompletableFuture<>();
        try (ExecWatch execWatch = client.pods().inNamespace(namespace)
                .withName(podName)
                .readingInput(null)
                .writingOutput(baos)
                .usingListener(new ExecListener() {
                    @Override
                    public void onOpen(Response response) {
                        LOGGER.info("Reading data...");
                    }

                    @Override
                    public void onFailure(Throwable throwable, Response response) {
                        data.completeExceptionally(throwable);
                    }

                    @Override
                    public void onClose(int i, String s) {
                        data.complete(baos.toString());
                    }
                }).exec(command)) {
            return data.get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            LOGGER.warn("Exception running command {} on pod: {}", command, e.getMessage());
            return "";
        }
    }

    public String execInPodContainer(String podName, String container, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CompletableFuture<String> data = new CompletableFuture<>();
        try (ExecWatch execWatch = client.pods().inNamespace(namespace)
                .withName(podName).inContainer(container)
                .readingInput(null)
                .writingOutput(baos)
                .usingListener(new ExecListener() {
                    @Override
                    public void onOpen(Response response) {
                        LOGGER.info("Reading data...");
                    }

                    @Override
                    public void onFailure(Throwable throwable, Response response) {
                        data.completeExceptionally(throwable);
                    }

                    @Override
                    public void onClose(int i, String s) {
                        data.complete(baos.toString());
                    }
                }).exec(command)) {
            return data.get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            LOGGER.warn("Exception running command {} on pod: {}", command, e.getMessage());
            return "";
        }
    }

    public void waitForPodDeletion(String namespace, String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("pod " + name + " deletion", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT,
                () -> client.pods().inNamespace(namespace).withName(name).get() == null);
    }

    public void waitForPodDeletion(String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("pod " + name + " deletion", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT,
                () -> client.pods().inNamespace(namespace).withName(name).get() == null);
    }


    public void waitForPod(String name) {
        LOGGER.info("Waiting when Pod {} will be ready", name);

        TestUtils.waitFor("pod " + name + " will be ready", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT,
                () -> {
                    List<ContainerStatus> statuses =  client.pods().inNamespace(namespace).withName(name).get().getStatus().getContainerStatuses();
                    for (ContainerStatus containerStatus : statuses) {
                        if (!containerStatus.getReady()) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    public List<Pod> listPods(String namespace, LabelSelector selector) {
        return client.pods().inNamespace(namespace).withLabelSelector(selector).list().getItems();
    }

    public List<Pod> listPods(Map<String, String> labelSelector) {
        return client.pods().withLabels(labelSelector).list().getItems();
    }

    public List<Pod> listPods(String namespace) {
        return client.pods().inNamespace(namespace).list().getItems();
    }

    public List<Pod> listPods() {
        return client.pods().list().getItems();
    }

    /**
     * Gets pod
     */
    public Pod getPod(String namespace, String name) {
        return client.pods().inNamespace(namespace).withName(name).get();
    }

    /**
     * Gets stateful set
     */
    public StatefulSet getStatefulSet(String namespace, String statefulSetName) {
        return  client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).get();
    }

    /**
     * Gets stateful set selectors
     */
    public LabelSelector getStatefulSetSelectors(String namespace, String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).get().getSpec().getSelector();
    }

    /**
     * Gets stateful set status
     */
    public boolean getStatefulSetStatus(String namespace, String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).isReady();
    }

    /**
     * Gets deployment
     */
    public Deployment getDeployment(String namespace, String deploymentName) {
        return client.extensions().deployments().inNamespace(namespace).withName(deploymentName).get();
    }

    /**
     * Gets deployment status
     */
    public LabelSelector getDeploymentSelectors(String namespace, String deploymentName) {
        return client.extensions().deployments().inNamespace(namespace).withName(deploymentName).get().getSpec().getSelector();
    }

    /**
     * Gets deployment status
     */
    public boolean getDeploymentStatus(String namespace, String deploymentName) {
        return client.extensions().deployments().inNamespace(namespace).withName(deploymentName).isReady();
    }

    /**
     * Gets deployment config status
     */
    public boolean getDeploymentConfigStatus(String namespace, String deploymentCofigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(namespace).withName(deploymentCofigName).isReady();
    }

    public Secret createSecret(Secret secret) {
        return client.secrets().create(secret);
    }

    public Secret patchSecret(String secretName, Secret secret) {
        return client.secrets().withName(secretName).patch(secret);
    }


    public Secret getSecret(String secretName) {
        return client.secrets().withName(secretName).get();
    }

    public List<Secret> getListSecrets() {
        return client.secrets().list().getItems();
    }

    public String logs(String podName, String containerName) {
        if (containerName != null) {
            return client.pods().withName(podName).inContainer(containerName).getLog();
        } else {
            return client.pods().withName(podName).getLog();
        }
    }

    public List<Event> getEvents(String resourceType, String resourceName) {
        return client.events().inNamespace(namespace).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getKind().equals(resourceType))
                .filter(event -> event.getInvolvedObject().getName().equals(resourceName))
                .collect(Collectors.toList());
    }
}
