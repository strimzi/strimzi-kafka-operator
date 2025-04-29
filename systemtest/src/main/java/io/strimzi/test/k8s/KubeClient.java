/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolume;
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
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
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
import io.skodjob.testframe.resources.KubeResourceManager;
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

    // =========================
    // ---------> POD <---------
    // =========================
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

    public List<String> listPodNames(String namespaceName, LabelSelector labelSelector) {
        return listPods(namespaceName, labelSelector).stream()
            .map(pod -> pod.getMetadata().getName())
            .collect(Collectors.toList());
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

    public List<String> listPodNames(String namespaceName, String clusterName, String key, String value) {
        return listPods(namespaceName, Collections.singletonMap(key, value)).stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName))
            .map(pod -> pod.getMetadata().getName())
            .collect(Collectors.toList());
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

    // ================================
    // ---------> DEPLOYMENT <---------
    // ================================

    public String getDeploymentNameByPrefix(String deploymentNamePrefix) {
        return getDeploymentNameByPrefix(getNamespace(), deploymentNamePrefix);
    }

    public String getDeploymentNameByPrefix(String namespaceName, String deploymentNamePrefix) {
        List<Deployment> prefixDeployments = client.apps().deployments().inNamespace(namespaceName).list().getItems().stream().filter(
            rs -> rs.getMetadata().getName().startsWith(deploymentNamePrefix)).collect(Collectors.toList());

        if (prefixDeployments.size() > 0) {
            return prefixDeployments.get(0).getMetadata().getName();
        } else {
            return null;
        }
    }

    /**
     * Gets deployment status
     */
    public LabelSelector getDeploymentSelectors(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).get().getSpec().getSelector();
    }

    /**
     * Gets deployment status
     */
    public boolean getDeploymentStatus(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).isReady();
    }

    public void deleteJob(final String namespaceName, String jobName) {
        client.batch().v1().jobs().inNamespace(namespaceName).withName(jobName).delete();
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

    public void patchSecret(String namespaceName, String secretName, Secret secret) {
        client.secrets().inNamespace(namespaceName).withName(secretName).patch(PatchContext.of(PatchType.JSON), secret);
    }

    public void deleteSecret(String namespaceName, String secretName) {
        client.secrets().inNamespace(namespaceName).withName(secretName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public List<Secret> listSecrets(String namespaceName) {
        return client.secrets().inNamespace(namespaceName).list().getItems();
    }

    public List<Secret> listSecrets() {
        return listSecrets(getNamespace());
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

    // =====================================================
    // ---------------> OPENSHIFT RESOURCES <---------------
    // =====================================================

    public InstallPlan getNonApprovedInstallPlanForCsvNameOrPrefix(String namespaceName, String csvNameOrPrefix) {
        return client.adapt(OpenShiftClient.class).operatorHub().installPlans()
            .inNamespace(namespaceName).list().getItems().stream()
            .filter(installPlan -> !installPlan.getSpec().getApproved()
                && installPlan.getSpec().getClusterServiceVersionNames().get(0).contains(csvNameOrPrefix)).findFirst().get();
    }

    public ClusterServiceVersion getCsv(String namespaceName, String csvName) {
        return client.adapt(OpenShiftClient.class).operatorHub().clusterServiceVersions().inNamespace(namespaceName).withName(csvName).get();
    }

    public ClusterServiceVersion getCsvWithPrefix(String namespaceName, String csvPrefix) {
        return client.adapt(OpenShiftClient.class).operatorHub().clusterServiceVersions().inNamespace(namespaceName)
            .list().getItems().stream().filter(csv -> csv.getMetadata().getName().contains(csvPrefix)).findFirst().get();
    }


    // =============================================
    // ---------> PERSISTENT VOLUME CLAIM <---------
    // =============================================

    public PersistentVolumeClaim getPersistentVolumeClaim(String namespaceName, String pvcName) {
        return client.persistentVolumeClaims().inNamespace(namespaceName).withName(pvcName).get();
    }

    public List<PersistentVolumeClaim> listPersistentVolumeClaims(String namespaceName, String clusterName) {
        return client.persistentVolumeClaims().inNamespace(namespaceName).list().getItems().stream()
            .filter(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName))
            .collect(Collectors.toList());
    }

    // =======================================
    // ---------> PERSISTENT VOLUME <---------
    // =======================================

    public PersistentVolume getPersistentVolumeWithName(String pvName) {
        return client.persistentVolumes().withName(pvName).get();
    }

    public List<PersistentVolume> listClaimedPersistentVolumes(String namespaceName, String clusterName) {
        return client.persistentVolumes().list().getItems().stream()
            .filter(pv -> {
                boolean containsClusterName = pv.getSpec().getClaimRef().getName().contains(clusterName);
                boolean containsClusterNamespace = pv.getSpec().getClaimRef().getNamespace().contains(namespaceName);
                return containsClusterName && containsClusterNamespace;
            })
            .collect(Collectors.toList());
    }
}
