/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.admissionregistration.v1.ValidatingWebhookConfiguration;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.VersionInfo;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.ClusterServiceVersion;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.InstallPlan;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.InstallPlanBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KubeClient {

    private static final Logger LOGGER = LogManager.getLogger(KubeClient.class);
    protected final KubernetesClient client;
    protected String namespace;

    public KubeClient(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    // ============================
    // ---------> CLIENT <---------
    // ============================

    public KubernetesClient getClient() {
        return client;
    }

    // ===============================
    // ---------> NAMESPACE <---------
    // ===============================

    public KubeClient namespace(String futureNamespace) {
        return new KubeClient(this.client, futureNamespace);
    }

    public String getNamespace() {
        return namespace;
    }

    public Namespace getNamespace(String namespace) {
        return client.namespaces().withName(namespace).get();
    }

    public void createNamespace(String name) {
        Namespace ns = new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build();
        client.namespaces().resource(ns).createOrReplace();
    }

    public void createOrReplaceNamespace(final Namespace namespace) {
        client.namespaces().resource(namespace).createOrReplace();
    }

    public void deleteNamespace(String name) {
        client.namespaces().withName(name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    // ================================
    // ---------> CONFIG MAP <---------
    // ================================

    public void createOrReplaceConfigMap(ConfigMap configMap) {
        client.configMaps().inNamespace(configMap.getMetadata().getNamespace()).resource(configMap).createOrReplace();
    }

    public void deleteConfigMap(ConfigMap configMap) {
        client.configMaps().inNamespace(configMap.getMetadata().getNamespace()).withName(configMap.getMetadata().getName()).delete();
    }

    public void deleteConfigMap(String configMapName) {
        client.configMaps().inNamespace(getNamespace()).withName(configMapName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public ConfigMap getConfigMap(String namespaceName, String configMapName) {
        return client.configMaps().inNamespace(namespaceName).withName(configMapName).get();
    }

    public ConfigMap getConfigMap(String configMapName) {
        return getConfigMap(getNamespace(), configMapName);
    }

    /**
     * Gets config map uid
     * @param configMapName config map name
     * @return config map ui
     */
    public String getConfigMapUid(String configMapName) {
        return getConfigMap(configMapName).getMetadata().getUid();
    }

    public List<ConfigMap> listConfigMapsInSpecificNamespace(String namespaceName, String namePrefix) {
        return client.configMaps().inNamespace(namespaceName).list().getItems().stream()
            .filter(cm -> cm.getMetadata().getName().startsWith(namePrefix))
            .collect(Collectors.toList());
    }

    public List<ConfigMap> listConfigMaps(String namePrefix) {
        return listConfigMapsInSpecificNamespace(getNamespace(), namePrefix);
    }

    public List<ConfigMap> listConfigMaps() {
        return client.configMaps().inNamespace(getNamespace()).list().getItems();
    }

    // =========================
    // ---------> POD <---------
    // =========================

    public PodResource editPod(String podName) {
        return editPod(getNamespace(), podName);
    }

    public PodResource editPod(String namespaceName, String podName) {
        return client.pods().inNamespace(namespaceName).withName(podName);
    }

    public String execInPod(String podName, String container, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CountDownLatch execLatch = new CountDownLatch(1);

        try {
            client.pods().inNamespace(getNamespace())
                .withName(podName).inContainer(container)
                .redirectingInput()
                .writingOutput(baos)
                .usingListener(new SimpleListener(execLatch))
                .exec(command);
            boolean wait = execLatch.await(1, TimeUnit.MINUTES);
            if (wait) {
                LOGGER.info("Await for command execution was finished");
            }
            return baos.toString(StandardCharsets.UTF_8);
        } catch (InterruptedException e) {
            LOGGER.warn("Exception running command {} on pod: {}", command, e.getMessage());
        }

        return "";
    }

    public List<Pod> listPods(LabelSelector selector) {
        return client.pods().inNamespace(getNamespace()).withLabelSelector(selector).list().getItems();
    }

    public List<Pod> listPods(String namespaceName, LabelSelector selector) {
        return client.pods().inNamespace(namespaceName).withLabelSelector(selector).list().getItems();
    }

    public List<Pod> listPods(Map<String, String> labelSelector) {
        return client.pods().inNamespace(getNamespace()).withLabels(labelSelector).list().getItems();
    }

    public List<Pod> listPods(String namespaceName, Map<String, String> labelSelector) {
        return client.pods().inNamespace(namespaceName).withLabels(labelSelector).list().getItems();
    }

    public List<Pod> listPods(String key, String value) {
        return listPods(Collections.singletonMap(key, value));
    }

    public List<Pod> listPods(String namespaceName, String clusterName, String key, String value) {
        return listPods(namespaceName, Collections.singletonMap(key, value)).stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName)).collect(Collectors.toList());
    }

    public List<Pod> listPods(String clusterName, String key, String value) {
        return listPods(getNamespace(), clusterName, key, value);
    }

    public List<String> listPodNames(String key, String value) {
        return listPods(Collections.singletonMap(key, value)).stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());
    }

    public List<String> listPodNames(String clusterName, String key, String value) {
        return listPods(Collections.singletonMap(key, value)).stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName))
            .map(pod -> pod.getMetadata().getName())
            .collect(Collectors.toList());
    }

    public List<String> listPodNamesInSpecificNamespace(String namespaceName, String key, String value) {
        return listPods(namespaceName, Collections.singletonMap(key, value)).stream()
            .map(pod -> pod.getMetadata().getName())
            .collect(Collectors.toList());
    }

    public PersistentVolumeClaim getPersistentVolumeClaim(String namespaceName, String pvcName) {
        return client.persistentVolumeClaims().inNamespace(namespaceName).withName(pvcName).get();
    }

    public void deletePersistentVolumeClaim(String namespaceName, String pvcName) {
        client.persistentVolumeClaims().inNamespace(namespaceName).withName(pvcName).delete();
    }

    public List<PersistentVolumeClaim> listPersistentVolumeClaims(String namespaceName, String clusterName) {
        return client.persistentVolumeClaims().inNamespace(namespaceName).list().getItems().stream()
            .filter(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName))
            .collect(Collectors.toList());
    }

    public List<String> listPodNames(String namespaceName, String clusterName, String key, String value) {
        return listPods(namespaceName, Collections.singletonMap(key, value)).stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName))
            .map(pod -> pod.getMetadata().getName())
            .collect(Collectors.toList());
    }

    public List<PersistentVolumeClaim> listPersistentVolumeClaims() {
        return client.persistentVolumeClaims().inNamespace(getNamespace()).list().getItems();
    }

    public List<Pod> listPods() {
        return listPods(getNamespace());
    }

    public List<Pod> listPods(String namespaceName) {
        return client.pods().inNamespace(namespaceName).list().getItems();
    }

    public List<Pod> listPodsByNamespace(String namespaceName, String clusterName) {
        return client.pods().inNamespace(namespaceName).list().getItems().stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName))
            .collect(Collectors.toList());
    }

    /**
     * Returns list of pods by prefix in pod name
     * @param namespaceName Namespace name
     * @param podNamePrefix prefix with which the name should begin
     * @return List of pods
     */
    public List<Pod> listPodsByPrefixInName(String namespaceName, String podNamePrefix) {
        return listPods(namespaceName)
                .stream().filter(p -> p.getMetadata().getName().startsWith(podNamePrefix))
                .collect(Collectors.toList());
    }

    public List<Pod> listPodsByPrefixInName(String podNamePrefix) {
        return listPodsByPrefixInName(getNamespace(), podNamePrefix);
    }

    /**
     * Gets pod
     */
    public Pod getPod(String namespaceName, String name) {
        return client.pods().inNamespace(namespaceName).withName(name).get();
    }

    public Pod getPod(String name) {
        return getPod(getNamespace(), name);
    }

    /**
     * Gets pod
     */
    public PodResource getPodResource(String namespaceName, String name) {
        return client.pods().inNamespace(namespaceName).withName(name);
    }

    /**
     * Gets pod Uid
     */
    public String getPodUid(String name) {
        return getPodUid(getNamespace(), name);
    }

    public String getPodUid(String namespace, String name) {
        return client.pods().inNamespace(namespace).withName(name).get().getMetadata().getUid();
    }

    /**
     * Deletes pod
     */
    public void deletePod(String namespaceName, Pod pod) {
        client.pods().inNamespace(namespaceName).resource(pod).delete();
    }

    public void deletePodWithName(String namespaceName, String podName) {
        client.pods().inNamespace(namespaceName).withName(podName).delete();
    }

    public void deletePod(Pod pod) {
        deletePod(getNamespace(), pod);
    }

    /**
     * Deletes pod
     */
    public void deletePodsByLabelSelector(LabelSelector labelSelector) {
        client.pods().inNamespace(getNamespace()).withLabelSelector(labelSelector).delete();
    }

    // ==================================
    // ---------> STATEFUL SET <---------
    // ==================================

    /**
     * Gets stateful set
     */
    public StatefulSet getStatefulSet(String namespaceName, String statefulSetName) {
        return  client.apps().statefulSets().inNamespace(namespaceName).withName(statefulSetName).get();
    }

    public StatefulSet getStatefulSet(String statefulSetName) {
        return getStatefulSet(getNamespace(), statefulSetName);
    }

    /**
     * Gets stateful set
     */
    public RollableScalableResource<StatefulSet> statefulSet(String namespaceName, String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespaceName).withName(statefulSetName);
    }

    public RollableScalableResource<StatefulSet> statefulSet(String statefulSetName) {
        return statefulSet(getNamespace(), statefulSetName);
    }

    /**
     * Gets stateful set selectors
     */
    public LabelSelector getStatefulSetSelectors(String namespaceName, String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespaceName).withName(statefulSetName).get().getSpec().getSelector();
    }

    /**
     * Gets stateful set status
     */
    public boolean getStatefulSetStatus(String namespaceName, String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespaceName).withName(statefulSetName).isReady();
    }

    /**
     * Gets stateful set Uid
     */
    public String getStatefulSetUid(String statefulSetName) {
        return getStatefulSet(statefulSetName).getMetadata().getUid();
    }

    public void deleteStatefulSet(String statefulSetName) {
        client.apps().statefulSets().inNamespace(getNamespace()).withName(statefulSetName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    // ================================
    // ---------> DEPLOYMENT <---------
    // ================================

    /**
     * Gets deployment
     */
    public Deployment getDeployment(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).get();
    }

    public String getDeploymentNameByPrefix(String namePrefix) {
        List<Deployment> prefixDeployments = client.apps().deployments().inNamespace(getNamespace()).list().getItems().stream().filter(
            rs -> rs.getMetadata().getName().startsWith(namePrefix)).collect(Collectors.toList());

        if (prefixDeployments.size() > 0) {
            return prefixDeployments.get(0).getMetadata().getName();
        } else {
            return null;
        }
    }

    /**
     * Gets deployment UID
     */
    public String getDeploymentUid(String namespaceName, String deploymentName) {
        return getDeployment(namespaceName, deploymentName).getMetadata().getUid();
    }

    /**
     * Gets deployment status
     */
    public LabelSelector getDeploymentSelectors(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).get().getSpec().getSelector();
    }

    public LabelSelector getDeploymentSelectors(String deploymentName) {
        return getDeploymentSelectors(getNamespace(), deploymentName);
    }

    /**
     * Gets deployment status
     */
    public boolean getDeploymentStatus(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).isReady();
    }

    public void deleteDeployment(String namespaceName, String deploymentName) {
        client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public void createOrReplaceDeployment(Deployment deployment) {
        client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).resource(deployment).createOrReplace();
    }

    // ==========================
    // ---------> NODE <---------
    // ==========================

    public String getNodeAddress() {
        return kubeClient(namespace).listNodes().get(0).getStatus().getAddresses().get(0).getAddress();
    }

    public List<Node> listNodes() {
        return client.nodes().list().getItems();
    }

    public List<Node> listWorkerNodes() {
        return listNodes().stream().filter(node -> node.getMetadata().getLabels().containsKey("node-role.kubernetes.io/worker")).collect(Collectors.toList());
    }

    /**
     * Method which return list of kube cluster nodes
     * @return list of nodes
     */
    public List<Node> getClusterNodes() {
        return client.nodes().list().getItems();
    }

    // =========================
    // ---------> JOB <---------
    // =========================


    public void createJob(Job job) {
        client.batch().v1().jobs().inNamespace(job.getMetadata().getNamespace()).resource(job).createOrReplace();
    }

    public void deleteJob(String jobName) {
        client.batch().v1().jobs().inNamespace(getNamespace()).withName(jobName).delete();
    }

    public void deleteJob(final String namespaceName, String jobName) {
        client.batch().v1().jobs().inNamespace(namespaceName).withName(jobName).delete();
    }

    public Job getJob(String jobName) {
        return client.batch().v1().jobs().inNamespace(getNamespace()).withName(jobName).get();
    }

    public Job getJob(final String namespaceName, String jobName) {
        return client.batch().v1().jobs().inNamespace(namespaceName).withName(jobName).get();
    }

    public boolean checkSucceededJobStatus(String namespaceName, String jobName, int expectedSucceededPods) {
        JobStatus jobStatus = getJobStatus(namespaceName, jobName);
        return jobStatus != null && jobStatus.getSucceeded() != null && jobStatus.getSucceeded().equals(expectedSucceededPods);
    }

    public boolean checkFailedJobStatus(String namespaceName, String jobName, int expectedFailedPods) {
        JobStatus jobStatus = getJobStatus(namespaceName, jobName);
        return jobStatus != null && jobStatus.getFailed() != null && jobStatus.getFailed().equals(expectedFailedPods);
    }

    // Pods Statuses:  0 Running / 0 Succeeded / 1 Failed
    public JobStatus getJobStatus(String namespaceName, String jobName) {
        Job job = client.batch().v1().jobs().inNamespace(namespaceName).withName(jobName).get();
        return job == null ? null : job.getStatus();
    }

    public JobStatus getJobStatus(String jobName) {
        return getJobStatus(getNamespace(), jobName);
    }

    public JobList getJobList() {
        return client.batch().v1().jobs().inNamespace(getNamespace()).list();
    }

    // ============================
    // ---------> SECRET <---------
    // ============================

    public Secret createSecret(Secret secret) {
        return client.secrets().inNamespace(secret.getMetadata().getNamespace()).resource(secret).createOrReplace();
    }

    public void patchSecret(String namespaceName, String secretName, Secret secret) {
        client.secrets().inNamespace(namespaceName).withName(secretName).patch(PatchContext.of(PatchType.JSON), secret);
    }


    public Secret getSecret(String namespaceName, String secretName) {
        return client.secrets().inNamespace(namespaceName).withName(secretName).get();
    }

    public Secret getSecret(String secretName) {
        return getSecret(getNamespace(), secretName);
    }

    public void deleteSecret(String namespaceName, String secretName) {
        client.secrets().inNamespace(namespaceName).withName(secretName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public void deleteSecret(String secretName) {
        deleteSecret(getNamespace(), secretName);
    }

    public List<Secret> listSecrets(String namespaceName) {
        return client.secrets().inNamespace(namespaceName).list().getItems();
    }

    public List<Secret> listSecrets() {
        return listSecrets(getNamespace());
    }

    // =============================
    // ---------> SERVICE <---------
    // =============================

    public Service getService(String namespaceName, String serviceName) {
        return client.services().inNamespace(namespaceName).withName(serviceName).get();
    }

    public Service getService(String serviceName) {
        return getService(getNamespace(), serviceName);
    }

    public void createService(Service service) {
        client.services().inNamespace(service.getMetadata().getNamespace()).resource(service).createOrReplace();
    }

    /**
     * Gets service uid
     * @param serviceName service name
     * @return service uid
     */
    public String getServiceUid(String serviceName) {
        return getService(serviceName).getMetadata().getUid();
    }

    public List<Service> listServices(String namespaceName) {
        return client.services().inNamespace(namespaceName).list().getItems();
    }

    public List<Service> listServices() {
        return listServices(getNamespace());
    }

    public void deleteService(String serviceName) {
        client.services().inNamespace(getNamespace()).withName(serviceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public void deleteService(Service service) {
        client.services().inNamespace(getNamespace()).resource(service).delete();
    }

    public List<ServiceAccount> listServiceAccounts(String namespaceName) {
        return client.serviceAccounts().inNamespace(namespaceName).list().getItems();
    }

    public void createOrReplaceServiceAccount(ServiceAccount serviceAccount) {
        client.serviceAccounts().inNamespace(serviceAccount.getMetadata().getNamespace()).resource(serviceAccount).createOrReplace();
    }

    public void deleteServiceAccount(ServiceAccount serviceAccount) {
        client.serviceAccounts().inNamespace(serviceAccount.getMetadata().getNamespace()).resource(serviceAccount).delete();
    }

    public ServiceAccount getServiceAccount(String namespaceName, String name) {
        return client.serviceAccounts().inNamespace(namespaceName).withName(name).get();
    }

    // =========================
    // ---------> LOG <---------
    // =========================

    public String logs(String podName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).getLog();
    }

    public String logsInSpecificNamespace(String namespaceName, String podName, String containerName) {
        return client.pods().inNamespace(namespaceName).withName(podName).inContainer(containerName).getLog();
    }

    public String logs(String podName, String containerName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).inContainer(containerName).getLog();
    }

    public String logsInSpecificNamespace(String namespaceName, String podName) {
        return client.pods().inNamespace(namespaceName).withName(podName).getLog();
    }

    // ============================
    // ---------> EVENTS <---------
    // ============================

    public List<Event> listEvents() {
        return client.v1().events().inNamespace(getNamespace()).list().getItems();
    }

    public List<Event> listEventsByResourceUid(String resourceUid) {
        return listEvents().stream()
                .filter(event -> {
                    if (event.getInvolvedObject().getUid() == null) {
                        return false;
                    }
                    return event.getInvolvedObject().getUid().equals(resourceUid);
                })
                .collect(Collectors.toList());
    }

    // ===========================
    // ---------> ROLES <---------
    // ===========================

    public List<ClusterRole> listClusterRoles() {
        return client.rbac().clusterRoles().list().getItems();
    }

    public void createOrReplaceClusterRoles(ClusterRole clusterRole) {
        client.rbac().clusterRoles().resource(clusterRole).createOrReplace();
    }

    public void deleteClusterRole(ClusterRole clusterRole) {
        client.rbac().clusterRoles().resource(clusterRole).delete();
    }

    public ClusterRole getClusterRole(String name) {
        return client.rbac().clusterRoles().withName(name).get();
    }

    // ==================================
    // ---------> ROLE BINDING <---------
    // ==================================

    public void createOrReplaceRoleBinding(RoleBinding roleBinding) {
        client.rbac().roleBindings().inNamespace(getNamespace()).resource(roleBinding).createOrReplace();
    }

    public void createOrReplaceClusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        client.rbac().clusterRoleBindings().resource(clusterRoleBinding).createOrReplace();
    }

    public void deleteClusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        client.rbac().clusterRoleBindings().resource(clusterRoleBinding).delete();
    }

    public ClusterRoleBinding getClusterRoleBinding(String name) {
        return client.rbac().clusterRoleBindings().withName(name).get();
    }

    public List<RoleBinding> listRoleBindings(String namespaceName) {
        return client.rbac().roleBindings().inNamespace(namespaceName).list().getItems();
    }

    public RoleBinding getRoleBinding(String name) {
        return client.rbac().roleBindings().inNamespace(getNamespace()).withName(name).get();
    }

    public void deleteRoleBinding(String namespace, String name) {
        client.rbac().roleBindings().inNamespace(namespace).withName(name).delete();
    }

    public void createOrReplaceRole(Role role) {
        client.rbac().roles().inNamespace(getNamespace()).resource(role).createOrReplace();
    }

    public Role getRole(String name) {
        return client.rbac().roles().inNamespace(getNamespace()).withName(name).get();
    }

    public void deleteRole(String namespace, String name) {
        client.rbac().roles().inNamespace(namespace).withName(name).delete();
    }

    // =========================
    // ---------> CRD <---------
    // =========================

    private static class SimpleListener implements ExecListener {
        CountDownLatch execLatch;
        SimpleListener(CountDownLatch execLatch) {
            this.execLatch = execLatch;
            execLatch.countDown();
        }

        @Override
        public void onOpen() {
            LOGGER.info("The shell will remain open for 10 seconds.");
            execLatch.countDown();
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            try {
                LOGGER.info("shell barfed with code {} and body {}", response.code(), response.body());
            } catch (IOException e) {
                LOGGER.info("shell barfed with code {} and body() throws exception", response.code(), e);
            }

            execLatch.countDown();
        }

        @Override
        public void onClose(int code, String reason) {
            LOGGER.info("The shell will now close with error code {} by reason {}", code, reason);
            execLatch.countDown();
        }
    }

    // ====================================
    // ---------> NETWORK POLICY <---------
    // ====================================

    public NetworkPolicy getNetworkPolicy(String name) {
        return client.network().networkPolicies().inNamespace(getNamespace()).withName(name).get();
    }

    public void createNetworkPolicy(NetworkPolicy networkPolicy) {
        client.network().networkPolicies().inNamespace(getNamespace()).resource(networkPolicy).createOrReplace();
    }

    public void deleteNetworkPolicy(String name) {
        client.network().networkPolicies().inNamespace(getNamespace()).withName(name).delete();
    }

    // =====================================
    // ---> CUSTOM RESOURCE DEFINITIONS <---
    // =====================================

    public void createOrReplaceCustomResourceDefinition(CustomResourceDefinition resourceDefinition) {
        client.apiextensions().v1().customResourceDefinitions().resource(resourceDefinition).createOrReplace();
    }

    public void deleteCustomResourceDefinition(CustomResourceDefinition resourceDefinition) {
        client.apiextensions().v1().customResourceDefinitions().resource(resourceDefinition).delete();
    }

    public CustomResourceDefinition getCustomResourceDefinition(String name) {
        return client.apiextensions().v1().customResourceDefinitions().withName(name).get();
    }

    // ======================================
    // ---------> CLUSTER SPECIFIC <---------
    // ======================================

    /**
     * Method which return kubernetes version
     * @return kubernetes version
     */
    public String clusterKubernetesVersion() {
        // This is basically workaround cause this.client.getVersion() returns null every time
        VersionInfo versionInfo = new KubernetesClientBuilder().build().getKubernetesVersion();
        return versionInfo.getMajor() + "." + versionInfo.getMinor().replace("+", "");
    }

    /**
     * Method which return name of cluster operator pod
     * @return cluster operator pod name
     */
    public String getClusterOperatorPodName(final String namespaceName) {
        LabelSelector selector = kubeClient(namespaceName).getDeploymentSelectors(namespaceName, "strimzi-cluster-operator");
        return kubeClient(namespaceName).listPods(namespaceName, selector).get(0).getMetadata().getName();
    }

    public String getClusterOperatorPodName() {
        return getClusterOperatorPodName(getNamespace());
    }

    /**
     * Method which return list of kube cluster workers node
     * @return list of worker nodes
     */
    public List<Node> getClusterWorkers() {
        return getClusterNodes().stream().filter(node ->
                node.getMetadata().getLabels().containsKey("node-role.kubernetes.io/worker")).collect(Collectors.toList());
    }

    // ======================================================
    // ---------> VALIDATING WEBHOOK CONFIGURATION <---------
    // ======================================================

    public ValidatingWebhookConfiguration getValidatingWebhookConfiguration(String webhookName) {
        return client.admissionRegistration().v1().validatingWebhookConfigurations().withName(webhookName).get();
    }

    public void createValidatingWebhookConfiguration(ValidatingWebhookConfiguration validatingWebhookConfiguration) {
        client.admissionRegistration().v1().validatingWebhookConfigurations().resource(validatingWebhookConfiguration).createOrReplace();
    }

    public void deleteValidatingWebhookConfiguration(ValidatingWebhookConfiguration validatingWebhookConfiguration) {
        client.admissionRegistration().v1().validatingWebhookConfigurations().resource(validatingWebhookConfiguration).delete();
    }

    // =====================================================
    // ---------------> OPENSHIFT RESOURCES <---------------
    // =====================================================

    public String getInstallPlanNameUsingCsvPrefix(String namespaceName, String csvPrefix) {
        return client.adapt(OpenShiftClient.class).operatorHub().installPlans()
            .inNamespace(namespaceName).list().getItems().stream()
            .filter(installPlan -> installPlan.getSpec().getClusterServiceVersionNames().toString().contains(csvPrefix)).findFirst().get().getMetadata().getName();
    }

    public InstallPlan getInstallPlan(String namespaceName, String installPlanName) {
        return client.adapt(OpenShiftClient.class).operatorHub().installPlans().inNamespace(namespaceName).withName(installPlanName).get();
    }

    public void approveInstallPlan(String namespaceName, String installPlanName) {
        InstallPlan installPlan = new InstallPlanBuilder(kubeClient().getInstallPlan(namespaceName, installPlanName))
            .editSpec()
                .withApproved()
            .endSpec()
            .build();

        client.adapt(OpenShiftClient.class).operatorHub().installPlans().inNamespace(namespaceName).withName(installPlanName).patch(installPlan);
    }

    public InstallPlan getNonApprovedInstallPlan(String namespaceName) {
        return client.adapt(OpenShiftClient.class).operatorHub().installPlans()
            .inNamespace(namespaceName).list().getItems().stream().filter(installPlan -> !installPlan.getSpec().getApproved()).findFirst().get();
    }

    public ClusterServiceVersion getCsv(String namespaceName, String csvName) {
        return client.adapt(OpenShiftClient.class).operatorHub().clusterServiceVersions().inNamespace(namespaceName).withName(csvName).get();
    }

    public ClusterServiceVersion getCsvWithPrefix(String namespaceName, String csvPrefix) {
        return client.adapt(OpenShiftClient.class).operatorHub().clusterServiceVersions().inNamespace(namespaceName)
            .list().getItems().stream().filter(csv -> csv.getMetadata().getName().contains(csvPrefix)).findFirst().get();
    }

    public void deleteCsv(String namespaceName, String csvName) {
        client.adapt(OpenShiftClient.class).operatorHub().clusterServiceVersions().inNamespace(namespaceName).withName(csvName).delete();
    }
}
