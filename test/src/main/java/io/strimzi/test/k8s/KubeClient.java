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
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobList;
import io.fabric8.kubernetes.api.model.batch.JobStatus;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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

    public boolean namespaceExists(String namespace) {
        return client.namespaces().list().getItems().stream().map(n -> n.getMetadata().getName())
            .collect(Collectors.toList()).contains(namespace);
    }

    public void createNamespace(String name) {
        Namespace ns = new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build();
        client.namespaces().createOrReplace(ns);
    }

    public void deleteNamespace(String name) {
        client.namespaces().withName(name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Gets namespace status
     */
    public boolean getNamespaceStatus(String namespaceName) {
        return client.namespaces().withName(namespaceName).isReady();
    }

    // ================================
    // ---------> CONFIG MAP <---------
    // ================================

    public void deleteConfigMap(String configMapName) {
        client.configMaps().inNamespace(getNamespace()).withName(configMapName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public ConfigMap getConfigMap(String configMapName) {
        return client.configMaps().inNamespace(getNamespace()).withName(configMapName).get();
    }

    /**
     * Gets config map uid
     * @param configMapName config map name
     * @return config map ui
     */
    public String getConfigMapUid(String configMapName) {
        return getConfigMap(configMapName).getMetadata().getUid();
    }

    public boolean getConfigMapStatus(String configMapName) {
        return client.configMaps().inNamespace(getNamespace()).withName(configMapName).isReady();
    }

    public List<ConfigMap> listConfigMaps() {
        return client.configMaps().inNamespace(getNamespace()).list().getItems();
    }

    public List<ConfigMap> listConfigMaps(String namePrefix) {
        return listConfigMaps().stream()
                .filter(cm -> cm.getMetadata().getName().startsWith(namePrefix))
                .collect(Collectors.toList());
    }

    // =========================
    // ---------> POD <---------
    // =========================

    public EditReplacePatchDeletable<Pod> editPod(String podName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).withPropagationPolicy(DeletionPropagation.ORPHAN);
    }

    public String execInPod(String podName, String container, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CountDownLatch execLatch = new CountDownLatch(1);

        try {
            client.pods().inNamespace(getNamespace())
                .withName(podName).inContainer(container)
                .readingInput(null)
                .writingOutput(baos)
                .usingListener(new SimpleListener(execLatch))
                .exec(command);
            boolean wait = execLatch.await(1, TimeUnit.MINUTES);
            if (wait) {
                LOGGER.info("Await for command execution was finished");
            }
            return baos.toString("UTF-8");
        } catch (UnsupportedEncodingException | InterruptedException e) {
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

    public List<Pod> listPods(String key, String value) {
        return listPods(Collections.singletonMap(key, value));
    }

    public List<Pod> listPods(String clusterName, String key, String value) {
        return listPods(Collections.singletonMap(key, value)).stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName)).collect(Collectors.toList());
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

    public List<PersistentVolumeClaim> listPersistentVolumeClaims(String namespaceName, String clusterName) {
        return client.persistentVolumeClaims().inNamespace(namespaceName).list().getItems().stream()
            .filter(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName))
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

    public List<Pod> listKafkaConnectS2IPods(String namespaceName, String connectS2iClusterName) {
        return listPods(namespaceName)
            .stream().filter(p ->
                p.getMetadata().getName().startsWith(connectS2iClusterName) &&
                !p.getMetadata().getName().endsWith("-build") &&
                !p.getMetadata().getName().endsWith("-deploy"))
            .collect(Collectors.toList());
    }

    public List<Pod> listKafkaConnectS2IPods(String connectS2iClusterName) {
        return listKafkaConnectS2IPods(getNamespace(), connectS2iClusterName);
    }

    /**
     * Gets pod
     */
    public Pod getPod(String namespaceName, String name) {
        return client.pods().inNamespace(getNamespace()).withName(name).get();
    }

    public Pod getPod(String name) {
        return getPod(getNamespace(), name);
    }

    /**
     * Gets pod
     */
    public PodResource<Pod> getPodResource(String name) {
        return client.pods().inNamespace(getNamespace()).withName(name);
    }

    /**
     * Gets pod Uid
     */
    public String getPodUid(String name) {
        return client.pods().inNamespace(getNamespace()).withName(name).get().getMetadata().getUid();
    }

    /**
     * Deletes pod
     */
    public Boolean deletePod(Pod pod) {
        return client.pods().inNamespace(getNamespace()).delete(pod);
    }

    /**
     * Deletes pod
     */
    public Boolean deletePod(LabelSelector labelSelector) {
        return client.pods().inNamespace(getNamespace()).withLabelSelector(labelSelector).delete();
    }

    public Date getCreationTimestampForPod(String podName) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss'Z'");
        Pod pod = getPod(podName);
        Date parsedDate = null;
        try {
            parsedDate = df.parse(pod.getMetadata().getCreationTimestamp());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parsedDate;
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
    public LabelSelector getStatefulSetSelectors(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(getNamespace()).withName(statefulSetName).get().getSpec().getSelector();
    }

    /**
     * Gets stateful set status
     */
    public boolean getStatefulSetStatus(String namespaceName, String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespaceName).withName(statefulSetName).isReady();
    }

    public boolean getStatefulSetStatus(String statefulSetName) {
        return getStatefulSetStatus(getNamespace(), statefulSetName);
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

    public Deployment getDeployment(String deploymentName) {
        return getDeployment(kubeClient().getNamespace(), deploymentName);
    }

    public Deployment getDeploymentFromAnyNamespaces(String deploymentName) {
        return client.apps().deployments().inAnyNamespace().list().getItems().stream().filter(
            deployment -> deployment.getMetadata().getName().equals(deploymentName))
                .findFirst()
                .orElseThrow();
    }

    public String getDeploymentNameByPrefix(String namePrefix) {
        List<Deployment> prefixDeployments = client.apps().deployments().inNamespace(getNamespace()).list().getItems().stream().filter(
            rs -> rs.getMetadata().getName().startsWith(namePrefix)).collect(Collectors.toList());

        if (prefixDeployments != null && prefixDeployments.size() > 0) {
            return prefixDeployments.get(0).getMetadata().getName();
        } else {
            return null;
        }
    }

    public String getDeploymentFromAnyNamespaceBySubstring(String subString) {
        Deployment deployment = client.apps().deployments().inAnyNamespace().list().getItems().stream()
            .filter(rs -> rs.getMetadata().getName().contains(subString))
            .findFirst()
            .orElseThrow();
        return deployment.getMetadata().getName();
    }
    /**
     * Gets deployment UID
     */
    public String getDeploymentUid(String deploymentName) {
        return getDeployment(deploymentName).getMetadata().getUid();
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

    public boolean getDeploymentStatus(String deploymentName) {
        return getDeploymentStatus(kubeClient().getNamespace(), deploymentName);
    }

    public void deleteDeployment(String namespaceName, String deploymentName) {
        client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public void deleteDeployment(String deploymentName) {
        deleteDeployment(kubeClient().getNamespace(), deploymentName);
    }

    public Deployment createOrReplaceDeployment(Deployment deployment) {
        return client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).createOrReplace(deployment);
    }

    // =======================================
    // ---------> DEPLOYMENT CONFIG <---------
    // =======================================

    /**
     * Gets deployment config
     */
    public DeploymentConfig getDeploymentConfig(String namespaceName, String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(namespaceName).withName(deploymentConfigName).get();
    }

    /**
     * Gets deployment config
     */
    public DeploymentConfig getDeploymentConfig(String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).get();
    }

    /**
     * Gets deployment config selector
     */
    public Map<String, String> getDeploymentConfigSelectors(String namespaceName, String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(namespaceName).withName(deploymentConfigName).get().getSpec().getSelector();
    }

    /**
     * Gets deployment config selector
     */
    public Map<String, String> getDeploymentConfigSelectors(String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).get().getSpec().getSelector();
    }

    /**
     * Delete deployment config
     * @param deploymentConfigName deployment config name
     */
    public void deleteDeploymentConfig(String deploymentConfigName) {
        client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Gets deployment config status
     */
    public boolean getDeploymentConfigReadiness(String namespaceName, String deploymentConfigName) {
        return Boolean.TRUE.equals(client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(namespaceName).withName(deploymentConfigName).isReady());
    }

    public boolean getDeploymentConfigReadiness(String deploymentConfigName) {
        return getDeploymentConfigReadiness(kubeClient().getNamespace(), deploymentConfigName);
    }

    // ==================================
    // ---------> REPLICA SETS <---------
    // ==================================

    public String getReplicaSetNameByPrefix(String namePrefix) {
        return client.apps().replicaSets().inNamespace(getNamespace()).list().getItems().stream()
                .filter(rs -> rs.getMetadata().getName().startsWith(namePrefix)).collect(Collectors.toList()).get(0).getMetadata().getName();
    }

    public boolean replicaSetExists(String replicaSetName) {
        return client.apps().replicaSets().inNamespace(getNamespace()).list().getItems().stream().anyMatch(rs -> rs.getMetadata().getName().startsWith(replicaSetName));
    }

    public void deleteReplicaSet(String replicaSetName) {
        client.apps().replicaSets().inNamespace(getNamespace()).withName(replicaSetName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
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

    public boolean jobExists(String jobName) {
        return client.batch().jobs().inNamespace(getNamespace()).list().getItems().stream().anyMatch(j -> j.getMetadata().getName().startsWith(jobName));
    }

    public Job createJob(Job job) {
        return client.batch().jobs().inNamespace(job.getMetadata().getNamespace()).createOrReplace(job);
    }

    public Job replaceJob(Job job) {
        return client.batch().jobs().inNamespace(getNamespace()).createOrReplace(job);
    }

    public Boolean deleteJob(String jobName) {
        return client.batch().jobs().inNamespace(getNamespace()).withName(jobName).delete();
    }

    public Job getJob(String jobName) {
        return client.batch().jobs().inNamespace(getNamespace()).withName(jobName).get();
    }

    public Boolean checkSucceededJobStatus(String jobName) {
        return checkSucceededJobStatus(jobName, 1);
    }

    public Boolean checkSucceededJobStatus(String jobName, int expectedSucceededPods) {
        return getJobStatus(jobName).getSucceeded().equals(expectedSucceededPods);
    }

    // Pods Statuses:  0 Running / 0 Succeeded / 1 Failed
    public JobStatus getJobStatus(String jobName) {
        return client.batch().jobs().inNamespace(getNamespace()).withName(jobName).get().getStatus();
    }

    public JobList getJobList() {
        return client.batch().jobs().inNamespace(getNamespace()).list();
    }

    public List<Job> listJobs(String namePrefix) {
        return client.batch().jobs().inNamespace(getNamespace()).list().getItems().stream()
            .filter(job -> job.getMetadata().getName().startsWith(namePrefix)).collect(Collectors.toList());
    }

    // ============================
    // ---------> SECRET <---------
    // ============================

    public Secret createSecret(Secret secret) {
        return client.secrets().inNamespace(getNamespace()).createOrReplace(secret);
    }

    public Secret patchSecret(String secretName, Secret secret) {
        return client.secrets().inNamespace(getNamespace()).withName(secretName).patch(secret);
    }

    public Secret getSecret(String namespaceName, String secretName) {
        return client.secrets().inNamespace(getNamespace()).withName(secretName).get();
    }

    public Secret getSecret(String secretName) {
        return getSecret(kubeClient().getNamespace(), secretName);
    }

    public boolean deleteSecret(String secretName) {
        return client.secrets().inNamespace(getNamespace()).withName(secretName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public List<Secret> listSecrets() {
        return client.secrets().inNamespace(getNamespace()).list().getItems();
    }

    public List<Secret> listSecrets(String labelKey, String labelValue) {
        return listSecrets().stream()
            .filter(secret -> secret.getMetadata().getLabels() != null)
            .filter(secret -> secret.getMetadata().getLabels().containsKey(labelKey))
            .filter(secret -> secret.getMetadata().getLabels().containsValue(labelValue))
            .collect(Collectors.toList());
    }

    // =============================
    // ---------> INGRESS <---------
    // =============================

    public Ingress createIngress(Ingress ingress) {
        return client.network().v1().ingresses().inNamespace(getNamespace()).createOrReplace(ingress);
    }

    public Boolean deleteIngress(Ingress ingress) {
        return client.network().v1().ingresses().inNamespace(getNamespace()).delete(ingress);
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

    public Service createService(Service service) {
        return client.services().inNamespace(getNamespace()).createOrReplace(service);
    }

    /**
     * Gets service uid
     * @param serviceName service name
     * @return service uid
     */
    public String getServiceUid(String serviceName) {
        return getService(serviceName).getMetadata().getUid();
    }

    public boolean getServiceStatus(String serviceName) {
        return client.services().inNamespace(getNamespace()).withName(serviceName).isReady();
    }

    public List<Service> listServices() {
        return client.services().inNamespace(getNamespace()).list().getItems();
    }

    public void deleteService(String serviceName) {
        client.services().inNamespace(getNamespace()).withName(serviceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public void deleteService(Service service) {
        client.services().inNamespace(getNamespace()).delete(service);
    }

    public List<ServiceAccount> listServiceAccounts() {
        return client.serviceAccounts().inNamespace(getNamespace()).list().getItems();
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

    public String logsInSpecificNamespace(String namespaceName, String podName) {
        return client.pods().inNamespace(namespaceName).withName(podName).getLog();
    }

    public String logs(String podName, String containerName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).inContainer(containerName).getLog();
    }

    // ============================
    // ---------> EVENTS <---------
    // ============================

    @SuppressWarnings("deprecation")
    public List<Event> listEvents() {
        return client.v1().events().inNamespace(getNamespace()).list().getItems();
    }

    @SuppressWarnings("deprecation")
    public List<Event> listEvents(String resourceType, String resourceName) {
        return client.v1().events().inNamespace(getNamespace()).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getKind().equals(resourceType))
                .filter(event -> event.getInvolvedObject().getName().equals(resourceName))
                .collect(Collectors.toList());
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

    public List<Role> listRoles() {
        return client.rbac().roles().list().getItems();
    }

    public List<ClusterRole> listClusterRoles() {
        return client.rbac().clusterRoles().list().getItems();
    }

    // ==================================
    // ---------> ROLE BINDING <---------
    // ==================================

    public RoleBinding createOrReplaceRoleBinding(RoleBinding roleBinding) {
        return client.rbac().roleBindings().inNamespace(getNamespace()).createOrReplace(roleBinding);
    }

    public ClusterRoleBinding createOrReplaceClusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        return client.rbac().clusterRoleBindings().inNamespace(getNamespace()).createOrReplace(clusterRoleBinding);
    }

    public Boolean deleteClusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        return client.rbac().clusterRoleBindings().inNamespace(getNamespace()).delete(clusterRoleBinding);
    }

    public ClusterRoleBinding getClusterRoleBinding(String name) {
        return client.rbac().clusterRoleBindings().inNamespace(getNamespace()).withName(name).get();
    }

    public List<RoleBinding> listRoleBindings() {
        return client.rbac().roleBindings().list().getItems();
    }

    public RoleBinding getRoleBinding(String name) {
        return client.rbac().roleBindings().inNamespace(getNamespace()).withName(name).get();
    }

    public void deleteRoleBinding(String name) {
        client.rbac().roleBindings().inNamespace(getNamespace()).withName(name).delete();
    }

    public <T extends CustomResource, L extends CustomResourceList<T>> MixedOperation<T, L, Resource<T>> customResources(CustomResourceDefinitionContext crdContext, Class<T> resourceType, Class<L> listClass) {
        return client.customResources(resourceType, listClass); //TODO namespace here
    }

    // =========================
    // ---------> CRD <---------
    // =========================

    public List<CustomResourceDefinition> listCustomResourceDefinition() {
        return client.apiextensions().v1beta1().customResourceDefinitions().list().getItems();
    }

    private static class SimpleListener implements ExecListener {
        CountDownLatch execLatch;
        SimpleListener(CountDownLatch execLatch) {
            this.execLatch = execLatch;
            execLatch.countDown();
        }

        @Override
        public void onOpen(Response response) {
            LOGGER.info("The shell will remain open for 10 seconds.");
            execLatch.countDown();
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            LOGGER.info("shell barfed with code {} and message {}", response.code(), response.message());
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

    public NetworkPolicy createNetworkPolicy(NetworkPolicy networkPolicy) {
        return client.network().networkPolicies().inNamespace(getNamespace()).createOrReplace(networkPolicy);
    }

    public void deleteNetworkPolicy(String name) {
        client.network().networkPolicies().inNamespace(getNamespace()).withName(name).delete();
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
        VersionInfo versionInfo = new DefaultKubernetesClient().getVersion();
        return versionInfo.getMajor() + "." + versionInfo.getMinor().replace("+", "");
    }

    /**
     * Method which return name of cluster operator pod
     * @return cluster operator pod name
     */
    public String getClusterOperatorPodName() {
        LabelSelector selector = kubeClient().getDeploymentSelectors("strimzi-cluster-operator");
        return kubeClient().listPods(selector).get(0).getMetadata().getName();
    }

    /**
     * Method which return list of kube cluster workers node
     * @return list of worker nodes
     */
    public List<Node> getClusterWorkers() {
        return getClusterNodes().stream().filter(node ->
                node.getMetadata().getLabels().containsKey("node-role.kubernetes.io/worker")).collect(Collectors.toList());
    }
}
